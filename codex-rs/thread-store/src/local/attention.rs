//! Strict receipt recovery through the canonical lineage and representation owners.
use super::LocalThreadStore;
use super::live_writer;
use super::rollout_lineage::RolloutLineageSegment;
use crate::AttentionReceiptCoverage;
use crate::CanonicalAttentionReceipts;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;
use codex_protocol::ThreadId;
use codex_protocol::protocol::EventMsg;
use codex_rollout::RolloutItem;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;

fn unavailable(detail: impl std::fmt::Display) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("canonical attention receipts unavailable: {detail}"),
    }
}

fn owners(thread_id: ThreadId, segments: &[RolloutLineageSegment]) -> Vec<ThreadId> {
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
    for _attempt in 0..2 {
        let proposed = store
            .resolve_attention_lineage(thread_id, path.clone())
            .await?;
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
        let (recorder, _, _) = live_writer::live_writer_parts(store, thread_id).await?;
        // Already under the target writer guard: no public helper may reacquire it.
        recorder.flush().await.map_err(unavailable)?;
        let captured = store
            .resolve_attention_lineage(thread_id, path.clone())
            .await?;
        if captured != verified {
            continue;
        }
        let segments = captured.segments;
        // Existing decoder-owned readers retain their open representation through
        // compression/materialization. Freeze finite decoded lengths under writers.
        let handles = tokio::task::spawn_blocking(move || {
            segments
                .into_iter()
                .map(|segment| {
                    let file = codex_rollout::open_rollout_seekable_prefix_reader(
                        &segment.rollout_path,
                        segment.end.map(|end| end.end_byte_offset),
                    )?;
                    let length = file.metadata()?.len();
                    let frontier = segment.end.map_or(length, |end| end.end_byte_offset);
                    if frontier > length {
                        return Err(std::io::Error::other("incomplete canonical prefix"));
                    }
                    Ok((segment, file, frontier))
                })
                .collect::<std::io::Result<Vec<_>>>()
        })
        .await
        .map_err(unavailable)?
        .map_err(unavailable)?;
        drop(writers);
        let result = tokio::task::spawn_blocking(move || scan(thread_id, handles))
            .await
            .map_err(unavailable)?;
        // Keep lifecycle leases through the entire strict scan. Revert/archive/
        // delete cannot replace or truncate a selected prefix while it is read.
        drop(leases);
        return result;
    }
    Err(unavailable(
        "canonical lineage changed during both capture attempts",
    ))
}

fn scan(
    thread_id: ThreadId,
    handles: Vec<(RolloutLineageSegment, File, u64)>,
) -> ThreadStoreResult<CanonicalAttentionReceipts> {
    let mut result = CanonicalAttentionReceipts {
        thread_id,
        notices: Vec::new(),
        coverage: Vec::new(),
    };
    for (segment, file, frontier) in handles {
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
                    paginated = meta.meta.history_mode
                        == codex_protocol::protocol::ThreadHistoryMode::Paginated;
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
    }
    if result.coverage.is_empty() {
        return Err(unavailable("empty canonical coverage"));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_protocol::protocol::{HistoryPosition, SessionMeta, SessionMetaLine};
    use codex_rollout::RolloutLine;
    use std::io::Write;
    fn bytes(id: ThreadId) -> Vec<u8> {
        let line = RolloutLine {
            timestamp: "2026-10-11T00:00:00Z".into(),
            ordinal: None,
            item: RolloutItem::SessionMeta(SessionMetaLine {
                meta: SessionMeta {
                    id,
                    ..Default::default()
                },
                git: None,
            }),
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
        let file = codex_rollout::open_rollout_seekable_reader(&path).unwrap();
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
