//! Strict receipt recovery through the canonical lineage and representation owners.
use super::LocalThreadStore;
use super::live_writer;
use super::rollout_lineage::RolloutLineage;
use super::rollout_lineage::RolloutLineageSegment;
use crate::AttentionPresentationScope;
use crate::AttentionReceiptCoverage;
use crate::CanonicalAttentionPresentation;
use crate::CanonicalAttentionReceipts;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;
use codex_app_server_protocol::ThreadHistoryBuilder;
use codex_app_server_protocol::ThreadItemPayload;
use codex_app_server_protocol::project_rollout_line;
use codex_protocol::ThreadId;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutItem;
#[cfg(test)]
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::sync::Arc;

fn unavailable(detail: impl std::fmt::Display) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("canonical attention receipts unavailable: {detail}"),
    }
}

pub(super) fn owners(thread_id: ThreadId, segments: &[RolloutLineageSegment]) -> Vec<ThreadId> {
    let mut ids = vec![thread_id];
    ids.extend(segments.iter().map(|segment| segment.owner_thread_id));
    ids.sort_unstable_by_key(ToString::to_string);
    ids.dedup();
    ids
}

pub(super) async fn load(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<CanonicalAttentionReceipts> {
    Ok(load_source(
        store,
        thread_id,
        false,
        #[cfg(test)]
        None,
    )
    .await?
    .receipts)
}

pub(super) async fn load_presentation(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<CanonicalAttentionPresentation> {
    load_source(
        store,
        thread_id,
        true,
        #[cfg(test)]
        None,
    )
    .await?
    .presentation
    .ok_or_else(|| unavailable("presentation purpose missing"))
}

struct SourceScan {
    receipts: CanonicalAttentionReceipts,
    presentation: Option<CanonicalAttentionPresentation>,
    paginated: bool,
}

async fn load_source(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    presentation: bool,
    #[cfg(test)] after_initial: Option<(
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    )>,
) -> ThreadStoreResult<SourceScan> {
    // A live recorder proves completed store acquisition. Materialize its lazy
    // header, never infer known-empty state from an absent file or SQL row.
    // This initial target-only barrier is released before acquiring the sorted
    // multi-owner set. No writer-to-lifecycle lock inversion is introduced.
    let path = {
        let _life = store.live_writer_locks.reserve_lifecycle(thread_id).await;
        let _writer = store.live_writer_locks.lock(thread_id).await;
        let (recorder, _, _) = live_writer::live_writer_parts(store, thread_id).await?;
        recorder.persist().await.map_err(unavailable)?;
        recorder.flush().await.map_err(unavailable)?;
        recorder.rollout_path().to_path_buf()
    };
    #[cfg(test)]
    if let Some((entered, release)) = after_initial {
        let _ = entered.send(());
        release.await.map_err(unavailable)?;
    }
    for _attempt in 0..2 {
        let proposed = store
            .resolve_attention_lineage(thread_id, path.clone())
            .await?;
        let scope = if presentation {
            Some(
                store
                    .resolve_attention_presentation_lineage(thread_id, path.clone())
                    .await?,
            )
        } else {
            None
        };
        let ids = owners(thread_id, proposed.segments());
        let mut leases = Vec::new();
        for id in &ids {
            leases.push(store.live_writer_locks.reserve_lifecycle(*id).await);
        }
        let verified = store
            .resolve_attention_lineage(thread_id, path.clone())
            .await?;
        if verified != proposed {
            continue;
        }
        let mut writers = Vec::new();
        for id in &ids {
            writers.push(store.live_writer_locks.lock(*id).await);
        }
        // Lifecycle leases are per-store. Retain native file ownership too for
        // presentation, including owners whose recorder can close during our scan.
        let mut native_owners = Vec::new();
        if presentation {
            for id in &ids {
                let live = store
                    .live_recorders
                    .lock()
                    .await
                    .get(id)
                    .map(|entry| Arc::clone(&entry.writer_lock));
                native_owners.push(match live {
                    Some(guard) => guard,
                    None => store.acquire_writer_lock(*id)?,
                });
            }
        }
        let (recorder, rollout_id, _) = live_writer::live_writer_parts(store, thread_id).await?;
        // The target-only barrier was released before sorted acquisition. Its
        // old immutable path can survive shutdown/revert/resume. Bind the fresh
        // acquired writer, not merely repeated reads of that old explicit path.
        if recorder.rollout_path() != path
            || verified.segments.last().is_none_or(|target| {
                target.rollout_id != rollout_id
                    || target.owner_thread_id != thread_id
                    || target.rollout_path != path
            })
        {
            return Err(ThreadStoreError::Conflict {
                message: "acquired attention persistence generation changed before capture".into(),
            });
        }
        // Already under the target writer guard: no public helper may reacquire it.
        recorder.flush().await.map_err(unavailable)?;
        let captured = store
            .resolve_attention_lineage(thread_id, path.clone())
            .await?;
        if captured != verified {
            continue;
        }
        if let Some(scope) = scope.as_ref()
            && *scope
                != store
                    .resolve_attention_presentation_lineage(thread_id, path.clone())
                    .await?
        {
            continue;
        }
        let protection = Arc::new((leases, native_owners));
        let capture_protection = Arc::clone(&protection);
        let (stop, keep_running) = tokio::sync::oneshot::channel::<()>();
        let stop = Arc::new(stop);
        let handles = tokio::task::spawn_blocking(move || {
            // Both sets are actually moved into this worker, not borrowed from
            // the async caller. Cancellation cannot release them during IO.
            let handles = capture_handles(captured, move || stop.is_closed());
            drop(writers);
            drop(capture_protection);
            handles
        })
        .await
        .map_err(unavailable)??;
        let mut result = scan_captured_source(
            thread_id,
            handles,
            false,
            scope.clone(),
            Arc::clone(&protection),
        )
        .await?;
        if let Some(view) = result.presentation.as_mut() {
            let bound = bind_presentation_scope(
                scope
                    .clone()
                    .ok_or_else(|| unavailable("native scope missing"))?,
                view,
            )?;
            if result.paginated {
                view.retained_turn_ids = positive_turn_ids(store, thread_id, &bound).await?;
            }
            if verified
                != store
                    .resolve_attention_lineage(thread_id, path.clone())
                    .await?
                || scope
                    != Some(
                        store
                            .resolve_attention_presentation_lineage(thread_id, path.clone())
                            .await?,
                    )
            {
                return Err(unavailable(
                    "presentation lineage changed during bounded read",
                ));
            }
        }
        drop(keep_running);
        return Ok(result);
    }
    Err(unavailable(
        "canonical lineage changed during both capture attempts",
    ))
}

fn bind_presentation_scope(
    mut scope: RolloutLineage,
    view: &CanonicalAttentionPresentation,
) -> ThreadStoreResult<RolloutLineage> {
    for segment in &mut scope.segments {
        let range = view
            .scope
            .iter()
            .find(|range| range.rollout_id == segment.rollout_id)
            .ok_or_else(|| unavailable("native scope outside physical coverage"))?;
        segment.start_ordinal = range.start_ordinal;
        segment.end = Some(HistoryPosition {
            thread_id: segment.rollout_id,
            end_ordinal_exclusive: range.end_ordinal_exclusive,
            end_byte_offset: range.end_byte_offset,
        });
    }
    Ok(scope)
}

async fn positive_turn_ids(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    bound: &RolloutLineage,
) -> ThreadStoreResult<std::collections::HashSet<String>> {
    let mut result = std::collections::HashSet::new();
    let mut cursor = None;
    let mut last_ordinal = None;
    loop {
        // Positive IDs only. ASC keeps every initial member reachable when
        // append-only projection moves a same-ID row forward.
        let page = super::thread_history::list_turns_on_lineage(
            store,
            crate::ListTurnsParams {
                thread_id,
                cursor,
                page_size: 128,
                sort_direction: crate::SortDirection::Asc,
                items_view: crate::StoredTurnItemsView::NotLoaded,
                include_archived: false,
            },
            bound,
        )
        .await?;
        result.extend(page.turns.into_iter().map(|turn| turn.turn_id));
        let Some(next) = page.next_cursor else {
            break;
        };
        let ordinal = super::thread_history::turn_cursor_ordinal(&next, thread_id)?;
        if last_ordinal.is_some_and(|last| ordinal <= last) {
            return Err(unavailable("native presentation cursor did not advance"));
        }
        last_ordinal = Some(ordinal);
        cursor = Some(next);
    }
    Ok(result)
}

/// Complete sorted protection for a closed replacement source.
pub(super) struct ReplacementGuards {
    _target: tokio::sync::OwnedRwLockWriteGuard<()>,
    _ancestors: Vec<tokio::sync::OwnedRwLockReadGuard<()>>,
    _writers: Vec<tokio::sync::OwnedMutexGuard<()>>,
}

impl ReplacementGuards {
    pub(super) fn target_only(
        target: tokio::sync::OwnedRwLockWriteGuard<()>,
        writer: tokio::sync::OwnedMutexGuard<()>,
    ) -> Self {
        Self {
            _target: target,
            _ancestors: Vec::new(),
            _writers: vec![writer],
        }
    }
}

pub(super) async fn lock_replacement(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    mut path: std::path::PathBuf,
) -> ThreadStoreResult<(
    Arc<ReplacementGuards>,
    super::rollout_lineage::RolloutLineage,
)> {
    for _attempt in 0..2 {
        if !tokio::fs::try_exists(&path).await.map_err(unavailable)?
            && let Some(current) =
                super::thread_rollout_resolver::resolve_current_including_archived(store, thread_id)
                    .await?
        {
            path = current.path;
        }

        let proposed = store
            .resolve_attention_lineage(thread_id, path.clone())
            .await?;
        let ids = owners(thread_id, proposed.segments());
        let mut target = None;
        let mut ancestors = Vec::new();
        for id in &ids {
            if *id == thread_id {
                target = Some(store.live_writer_locks.lock_lifecycle(*id).await);
            } else {
                ancestors.push(store.live_writer_locks.reserve_lifecycle(*id).await);
            }
        }
        let target = target.ok_or_else(|| unavailable("replacement target guard unavailable"))?;
        let mut writers = Vec::new();
        for id in &ids {
            writers.push(store.live_writer_locks.lock(*id).await);
        }
        let verified = store
            .resolve_attention_lineage(thread_id, path.clone())
            .await?;
        if verified != proposed {
            continue;
        }
        return Ok((
            Arc::new(ReplacementGuards {
                _target: target,
                _ancestors: ancestors,
                _writers: writers,
            }),
            verified,
        ));
    }
    Err(unavailable(
        "closed lineage changed during both capture attempts",
    ))
}

/// Strict proof carries the complete map, including retryable immutable bindings.
/// Each finite worker retains the caller's complete replacement protection.
pub(super) async fn preservation_under_guards(
    thread_id: ThreadId,
    lineage: super::rollout_lineage::RolloutLineage,
    allow_append_only_legacy: bool,
    protection: Arc<impl Send + Sync + 'static>,
) -> ThreadStoreResult<Vec<codex_protocol::attention::ThreadAttentionNotification>> {
    let snapshot =
        capture_under_guards(thread_id, lineage, allow_append_only_legacy, protection).await?;
    let reduced =
        crate::reduce_attention_receipts(thread_id, snapshot.notices).map_err(unavailable)?;
    Ok(crate::attention_receipts_for_preservation(reduced))
}

pub(super) async fn capture_under_guards(
    thread_id: ThreadId,
    lineage: super::rollout_lineage::RolloutLineage,
    allow_append_only_legacy: bool,
    protection: Arc<impl Send + Sync + 'static>,
) -> ThreadStoreResult<CanonicalAttentionReceipts> {
    let (stop, keep_running) = tokio::sync::oneshot::channel::<()>();
    let stop = Arc::new(stop);
    let capture_protection = Arc::clone(&protection);
    let handles = tokio::task::spawn_blocking(move || {
        let result = capture_handles(lineage, move || stop.is_closed());
        drop(capture_protection);
        result
    })
    .await
    .map_err(unavailable)??;
    let result = scan_captured(thread_id, handles, allow_append_only_legacy, protection).await;
    drop(keep_running);
    result
}

async fn scan_captured<R: Read + Send + 'static, P: Send + 'static>(
    thread_id: ThreadId,
    handles: Vec<(RolloutLineageSegment, R, u64)>,
    allow_append_only_legacy: bool,
    protection: P,
) -> ThreadStoreResult<CanonicalAttentionReceipts> {
    Ok(scan_captured_source(
        thread_id,
        handles,
        allow_append_only_legacy,
        None,
        protection,
    )
    .await?
    .receipts)
}

async fn scan_captured_source<R: Read + Send + 'static, P: Send + 'static>(
    thread_id: ThreadId,
    handles: Vec<(RolloutLineageSegment, R, u64)>,
    allow_append_only_legacy: bool,
    scope: Option<RolloutLineage>,
    protection: P,
) -> ThreadStoreResult<SourceScan> {
    tokio::task::spawn_blocking(move || {
        let result = scan_source(thread_id, handles, allow_append_only_legacy, scope);
        drop(protection);
        result
    })
    .await
    .map_err(unavailable)?
}

fn capture_handles(
    lineage: super::rollout_lineage::RolloutLineage,
    is_cancelled: impl Fn() -> bool + Clone + Send + 'static,
) -> ThreadStoreResult<Vec<(RolloutLineageSegment, impl Read + Seek + Send, u64)>> {
    // Freeze finite decoded lengths through the existing cancellable decoder.
    // No File downcast, metadata shortcut, or alternate parser.
    lineage
        .segments
        .into_iter()
        .map(|segment| {
            let mut reader = codex_rollout::open_rollout_seekable_prefix_reader(
                &segment.rollout_path,
                segment.end.map(|end| end.end_byte_offset),
                is_cancelled.clone(),
            )
            .map_err(unavailable)?;
            let length = reader.seek(SeekFrom::End(0)).map_err(unavailable)?;
            reader.rewind().map_err(unavailable)?;
            let frontier = segment.end.map_or(length, |end| end.end_byte_offset);
            if frontier > length {
                return Err(unavailable("incomplete canonical prefix"));
            }
            Ok((segment, reader, frontier))
        })
        .collect()
}

#[cfg(test)]
fn scan<R: Read>(
    thread_id: ThreadId,
    handles: Vec<(RolloutLineageSegment, R, u64)>,
) -> ThreadStoreResult<CanonicalAttentionReceipts> {
    scan_with_legacy(thread_id, handles, false)
}

#[cfg(test)]
fn scan_with_legacy<R: Read>(
    thread_id: ThreadId,
    handles: Vec<(RolloutLineageSegment, R, u64)>,
    allow_append_only_legacy: bool,
) -> ThreadStoreResult<CanonicalAttentionReceipts> {
    Ok(scan_source(thread_id, handles, allow_append_only_legacy, None)?.receipts)
}

fn scan_source<R: Read>(
    thread_id: ThreadId,
    handles: Vec<(RolloutLineageSegment, R, u64)>,
    allow_append_only_legacy: bool,
    scope: Option<RolloutLineage>,
) -> ThreadStoreResult<SourceScan> {
    let mut result = CanonicalAttentionReceipts {
        thread_id,
        notices: Vec::new(),
        coverage: Vec::new(),
    };
    let mut presentation = scope.as_ref().map(|_| CanonicalAttentionPresentation {
        thread_id,
        markers: Vec::new(),
        foreground_turn_ids: Default::default(),
        retained_turn_ids: Default::default(),
        coverage: Vec::new(),
        scope: Vec::new(),
    });
    let mut legacy_builder = ThreadHistoryBuilder::new();
    let mut target_paginated = false;
    // The newest target header is the lifecycle proof. An ancestor's version is
    // not a child's proof; an old inherited header cannot repair an unmarked target.
    let target_segment = handles
        .len()
        .checked_sub(1)
        .ok_or_else(|| unavailable("empty canonical coverage"))?;
    for (index, (segment, file, frontier)) in handles.into_iter().enumerate() {
        let visible = scope.as_ref().and_then(|scope| {
            scope
                .segments
                .iter()
                .find(|visible| visible.rollout_id == segment.rollout_id)
        });
        if let Some(visible) = visible
            && (visible.owner_thread_id != segment.owner_thread_id
                || visible.rollout_path != segment.rollout_path
                || visible.start_ordinal < segment.start_ordinal
                || visible.end != segment.end)
        {
            return Err(unavailable("native scope does not match physical coverage"));
        }
        let mut visible_start = visible.map(|range| range.start_ordinal);
        let mut reader = BufReader::new(file.take(frontier));
        let mut bytes = Vec::new();
        let mut offset = 0_u64;
        let mut ordinal = segment
            .start_ordinal
            .checked_sub(1)
            .ok_or_else(|| unavailable("invalid start ordinal"))?;
        let mut first = true;
        let mut paginated = false;
        loop {
            bytes.clear();
            let count = reader.read_until(b'\n', &mut bytes).map_err(unavailable)?;
            if count == 0 {
                break;
            }
            offset = offset
                .checked_add(count as u64)
                .ok_or_else(|| unavailable("byte offset overflow"))?;
            if bytes.last() != Some(&b'\n') {
                return Err(unavailable("truncated canonical record"));
            }
            let line = codex_rollout::parse_rollout_line_bytes(&bytes).map_err(unavailable)?;
            if first {
                if !matches!(&line.item, RolloutItem::SessionMeta(meta) if meta.meta.id == segment.owner_thread_id)
                {
                    return Err(unavailable("wrong or absent canonical session identity"));
                }
                if let RolloutItem::SessionMeta(meta) = &line.item {
                    if index == target_segment
                        && (segment.owner_thread_id != thread_id
                            || (presentation.is_none()
                                && !(meta.meta.attention_receipt_lifetime_version == Some(1)
                                    || (allow_append_only_legacy
                                        && meta.meta.attention_receipt_lifetime_version.is_none()
                                        && meta.meta.history_mode
                                            == codex_protocol::protocol::ThreadHistoryMode::Legacy
                                        && meta.meta.history_base.is_none()))))
                    {
                        return Err(unavailable("unknown or missing receipt lifetime version"));
                    }
                    paginated = meta.meta.history_mode
                        == codex_protocol::protocol::ThreadHistoryMode::Paginated;
                    if index == target_segment {
                        target_paginated = paginated;
                    }
                    if paginated && let Some(hidden) = meta.meta.subagent_history_start_ordinal {
                        visible_start = visible_start.map(|start| start.max(hidden));
                    }
                    if meta
                        .meta
                        .history_base
                        .map_or(0, |base| base.end_ordinal_exclusive)
                        != ordinal
                    {
                        return Err(unavailable("canonical metadata ordinal boundary changed"));
                    }
                }
                first = false;
            }
            // Copied forks legitimately retain source SessionMeta items after the
            // canonical first header. They are decoded data, never the target owner.
            if (paginated && line.ordinal != Some(ordinal))
                || line.ordinal.is_some_and(|value| value != ordinal)
            {
                return Err(unavailable("invalid canonical record ordinal"));
            }
            // Legacy's reducer owns native rollout indices as well as events.
            // Include its canonical header (ordinal base), not just visible rows.
            if presentation.is_some() && !paginated && visible.is_some() {
                legacy_builder.handle_rollout_item(&line.item);
            }
            if let Some(view) = presentation.as_mut()
                && visible_start.is_some_and(|start| ordinal >= start)
            {
                if let RolloutItem::EventMsg(EventMsg::AttentionTurnStarted(marker)) = &line.item
                    && marker.thread_id == thread_id.to_string()
                {
                    view.markers.push(marker.clone());
                }
                if paginated {
                    view.foreground_turn_ids.extend(
                        project_rollout_line(&line)
                            .changed_items
                            .into_iter()
                            .filter(|change| {
                                matches!(change.item.payload, ThreadItemPayload::UserMessage { .. })
                            })
                            .map(|change| change.turn_id),
                    );
                }
            }
            if let RolloutItem::EventMsg(EventMsg::ThreadAttention(notice)) = line.item {
                result.notices.push(notice);
            }
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| unavailable("ordinal overflow"))?;
        }
        if first
            || offset != frontier
            || segment
                .end
                .is_some_and(|end| ordinal != end.end_ordinal_exclusive)
        {
            return Err(unavailable("incomplete byte or ordinal coverage"));
        }
        result.coverage.push(AttentionReceiptCoverage {
            rollout_id: segment.rollout_id,
            owner_thread_id: segment.owner_thread_id,
            start_ordinal: segment.start_ordinal,
            end_ordinal_exclusive: ordinal,
            end_byte_offset: frontier,
        });
        if let Some(view) = presentation.as_mut()
            && let Some(start_ordinal) = visible_start
        {
            view.scope.push(AttentionPresentationScope {
                rollout_id: segment.rollout_id,
                owner_thread_id: segment.owner_thread_id,
                history_mode: if paginated {
                    ThreadHistoryMode::Paginated
                } else {
                    ThreadHistoryMode::Legacy
                },
                start_ordinal,
                end_ordinal_exclusive: ordinal,
                end_byte_offset: frontier,
            });
        }
    }
    if result.coverage.is_empty() {
        return Err(unavailable("empty canonical coverage"));
    }
    if let Some(view) = presentation.as_mut() {
        if scope
            .as_ref()
            .is_none_or(|scope| scope.segments.len() != view.scope.len())
        {
            return Err(unavailable("native scope missing from physical coverage"));
        }
        view.coverage = result.coverage.clone();
        if !target_paginated {
            for turn in legacy_builder.finish() {
                if turn
                    .items
                    .iter()
                    .any(|item| matches!(item.payload, ThreadItemPayload::UserMessage { .. }))
                {
                    view.foreground_turn_ids.insert(turn.id.clone());
                }
                view.retained_turn_ids.insert(turn.id);
            }
        }
    }
    Ok(SourceScan {
        receipts: result,
        presentation,
        paginated: target_paginated,
    })
}

#[cfg(test)]
#[path = "attention_presentation_tests.rs"]
mod presentation_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use codex_protocol::protocol::HistoryPosition;
    use codex_protocol::protocol::SessionMeta;
    use codex_protocol::protocol::SessionMetaLine;
    use codex_rollout::RolloutLine;
    use std::io::Write;
    pub(super) fn bytes(id: ThreadId) -> Vec<u8> {
        let line = RolloutLine {
            timestamp: "2026-10-11T00:00:00Z".into(),
            ordinal: None,
            item: RolloutItem::SessionMeta(Box::new(SessionMetaLine {
                meta: SessionMeta {
                    id,
                    attention_receipt_lifetime_version: Some(1),
                    ..Default::default()
                },
                git: None,
            })),
        };
        let mut bytes = serde_json::to_vec(&line).unwrap();
        bytes.push(b'\n');
        bytes
    }
    #[test]
    fn canonical_attention_capture_excludes_later_append_and_retains_replaced_representation() {
        let home = tempfile::tempdir().unwrap();
        let id = ThreadId::default();
        let physical = ThreadId::default();
        let path = home.path().join("rollout.jsonl");
        let prefix = bytes(id);
        std::fs::write(&path, &prefix).unwrap();
        let file = codex_rollout::open_rollout_seekable_reader(&path, || false).unwrap();
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"{invalid later suffix}\n")
            .unwrap();
        std::fs::rename(&path, home.path().join("old.jsonl")).unwrap();
        std::fs::write(&path, b"replacement\n").unwrap();
        let segment = RolloutLineageSegment {
            rollout_id: physical,
            owner_thread_id: id,
            rollout_path: path,
            start_ordinal: 1,
            end: None,
        };
        let recovered = scan(id, vec![(segment, file, prefix.len() as u64)]).unwrap();
        assert_eq!(recovered.coverage[0].rollout_id, physical);
        assert_eq!(recovered.coverage[0].owner_thread_id, id);
    }
    #[test]
    fn canonical_attention_wrong_identity_and_ordinal_cutoff_refuse() {
        let home = tempfile::tempdir().unwrap();
        let id = ThreadId::default();
        let other = ThreadId::default();
        let path = home.path().join("rollout.jsonl");
        let prefix = bytes(other);
        std::fs::write(&path, &prefix).unwrap();
        let mut segment = RolloutLineageSegment {
            rollout_id: id,
            owner_thread_id: id,
            rollout_path: path.clone(),
            start_ordinal: 1,
            end: None,
        };
        assert!(
            scan(
                id,
                vec![(
                    segment.clone(),
                    File::open(&path).unwrap(),
                    prefix.len() as u64
                )]
            )
            .is_err()
        );
        segment.owner_thread_id = other;
        segment.end = Some(HistoryPosition {
            thread_id: id,
            end_ordinal_exclusive: 2,
            end_byte_offset: prefix.len() as u64,
        });
        assert!(
            scan(
                id,
                vec![(segment, File::open(path).unwrap(), prefix.len() as u64)]
            )
            .is_err()
        );
    }
    #[test]
    fn canonical_attention_lifecycle_keys_use_sorted_logical_owners() {
        let id = ThreadId::default();
        let other = ThreadId::default();
        let physical = ThreadId::default();
        let segment = RolloutLineageSegment {
            rollout_id: physical,
            owner_thread_id: other,
            rollout_path: "unused".into(),
            start_ordinal: 1,
            end: None,
        };
        let keys = owners(id, &[segment.clone(), segment]);
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&id));
        assert!(keys.contains(&other));
        assert!(!keys.contains(&physical));
        assert!(keys[0].to_string() < keys[1].to_string());
    }
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;
    use std::io;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use tokio::sync::oneshot;

    struct Finished(Option<oneshot::Sender<()>>);
    impl Drop for Finished {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[test]
    fn cancelled_queued_capture_keeps_real_replacement_and_writer_fences() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let home = tempfile::tempdir().unwrap();
            let id = ThreadId::new();
            let store =
                LocalThreadStore::new(super::super::test_support::test_config(home.path()), None);
            let path = home.path().join("rollout.jsonl");
            let original = super::tests::bytes(id);
            std::fs::write(&path, &original).unwrap();
            let coordination = store.live_writer_locks.coordination(id).await;
            let guards = Arc::new(ReplacementGuards::target_only(
                store.live_writer_locks.lock_lifecycle(id).await,
                store.live_writer_locks.lock(id).await,
            ));
            let writer = store.acquire_writer_lock(id).unwrap();
            let (finished, stopped) = oneshot::channel();
            let protection = Arc::new((guards, writer, Finished(Some(finished))));
            let (entered, entry) = oneshot::channel();
            let (release, released) = std::sync::mpsc::channel::<()>();
            let blocker = tokio::task::spawn_blocking(move || {
                let _ = entered.send(());
                let _ = released.recv();
            });
            entry.await.unwrap();
            let lineage = super::super::rollout_lineage::RolloutLineage {
                segments: vec![RolloutLineageSegment {
                    rollout_id: id,
                    owner_thread_id: id,
                    rollout_path: path.clone(),
                    start_ordinal: 1,
                    end: None,
                }],
            };
            let mut work = Box::pin(capture_under_guards(id, lineage, false, protection));
            assert!(futures::poll!(&mut work).is_pending());
            drop(work);
            assert!(coordination.writer.try_lock().is_err());
            assert!(coordination.lifecycle.try_write().is_err());
            assert!(matches!(
                store.acquire_writer_lock(id),
                Err(ThreadStoreError::Conflict { .. })
            ));
            drop(release);
            blocker.await.unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(2), stopped)
                .await
                .unwrap()
                .unwrap();
            assert!(coordination.writer.try_lock().is_ok());
            assert!(coordination.lifecycle.try_write().is_ok());
            assert!(store.acquire_writer_lock(id).is_ok());
            assert_eq!(std::fs::read(path).unwrap(), original);
        });
    }

    struct PausedFile {
        file: File,
        entered: Option<oneshot::Sender<()>>,
        released: std::sync::mpsc::Receiver<()>,
        cancelled: Arc<AtomicBool>,
        refused: Arc<AtomicBool>,
    }
    impl Read for PausedFile {
        fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            if let Some(entered) = self.entered.take() {
                let _ = entered.send(());
                let _ = self.released.recv();
            }
            if self.cancelled.load(Ordering::SeqCst) {
                self.refused.store(true, Ordering::SeqCst);
                return Err(io::Error::other("test read cancelled, not EOF"));
            }
            self.file.read(bytes)
        }
    }

    #[tokio::test]
    async fn cancelled_running_scan_keeps_lifecycle_but_not_released_writers() {
        let home = tempfile::tempdir().unwrap();
        let id = ThreadId::new();
        let store =
            LocalThreadStore::new(super::super::test_support::test_config(home.path()), None);
        let path = home.path().join("rollout.jsonl");
        let original = super::tests::bytes(id);
        std::fs::write(&path, &original).unwrap();
        let lease = store.live_writer_locks.reserve_lifecycle(id).await;
        let native_owner = store.acquire_writer_lock(id).unwrap();
        let other =
            LocalThreadStore::new(super::super::test_support::test_config(home.path()), None);
        let coordination = store.live_writer_locks.coordination(id).await;
        let (entered, entry) = oneshot::channel();
        let (release, released) = std::sync::mpsc::channel::<()>();
        let (finished, stopped) = oneshot::channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        let refused = Arc::new(AtomicBool::new(false));
        let reader = PausedFile {
            file: File::open(&path).unwrap(),
            entered: Some(entered),
            released,
            cancelled: cancelled.clone(),
            refused: refused.clone(),
        };
        let segment = RolloutLineageSegment {
            rollout_id: id,
            owner_thread_id: id,
            rollout_path: path.clone(),
            start_ordinal: 1,
            end: None,
        };
        let work = tokio::spawn(scan_captured(
            id,
            vec![(segment, reader, original.len() as u64)],
            false,
            (lease, native_owner, Finished(Some(finished))),
        ));
        entry.await.unwrap();
        work.abort();
        assert!(work.await.unwrap_err().is_cancelled());
        assert!(
            coordination.writer.try_lock().is_ok(),
            "read-only scan does not retain the writer"
        );
        assert!(
            coordination.lifecycle.try_write().is_err(),
            "archive/replacement cannot pass running IO"
        );
        assert!(
            matches!(
                other.acquire_writer_lock(id),
                Err(ThreadStoreError::Conflict { .. })
            ),
            "another store cannot replace a source while the cancelled worker still reads"
        );
        cancelled.store(true, Ordering::SeqCst);
        drop(release);
        tokio::time::timeout(std::time::Duration::from_secs(2), stopped)
            .await
            .unwrap()
            .unwrap();
        assert!(refused.load(Ordering::SeqCst));
        assert!(coordination.lifecycle.try_write().is_ok());
        assert!(other.acquire_writer_lock(id).is_ok());
        assert_eq!(std::fs::read(path).unwrap(), original);
    }
}
