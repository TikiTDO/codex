use codex_protocol::ThreadId;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutConfig;
use codex_rollout::RolloutRecorder;
use codex_rollout::RolloutRecorderParams;

use super::LocalThreadStore;
use super::paginated_fork;
use super::thread_rollout_resolver;
use crate::ForkBoundary;
use crate::RevertThreadParams;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

/// Revert an unloaded paginated thread by creating a new immutable rollout file.
///
/// Old rollouts stay intact. The new file references the retained prefix, and the only mutable
/// cutover is the existing SQLite rollout-path pointer for the thread.
pub(super) async fn revert(
    store: &LocalThreadStore,
    params: RevertThreadParams,
) -> ThreadStoreResult<()> {
    let RevertThreadParams {
        thread_id,
        before_turn_id,
        multi_agent_version,
    } = params;
    let state_db = store
        .state_db()
        .await
        .ok_or(ThreadStoreError::Unsupported {
            operation: "revert_thread",
        })?;
    // Resolution may return a compressed sibling. Keep SQLite's exact stored path for the CAS.
    let stored_metadata = state_db
        .get_thread(thread_id)
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to read thread metadata for {thread_id}: {err}"),
        })?
        .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
    let expected_sqlite_path = stored_metadata.rollout_path;
    let current_rollout = thread_rollout_resolver::resolve_current(store, thread_id)
        .await?
        .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
    let source_path = current_rollout.path;
    // Discover the entire owner set before taking any lifecycle or writer lock.
    let (_replacement_guards, receipt_lineage) =
        super::attention::lock_replacement(store, thread_id, source_path.clone()).await?;
    store.ensure_live_recorder_absent(thread_id).await?;
    let writer_lock = store.acquire_writer_lock(thread_id)?;
    let verified_current = thread_rollout_resolver::resolve_current(store, thread_id)
        .await?
        .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
    if verified_current.path != source_path
        || verified_current.rollout_id != current_rollout.rollout_id
    {
        return Err(ThreadStoreError::Conflict {
            message: format!("thread {thread_id} changed before revert capture"),
        });
    }
    // Failed strict proof preserves the existing foreground path, never empty proof.
    let preserved_receipts = match super::attention::preservation_under_guards(
        thread_id,
        receipt_lineage,
        false,
    )
    .await
    {
        Ok(notices) => Some(notices),
        Err(error) => {
            tracing::warn!(
                thread_id = %thread_id,
                error = %error,
                "receipt preservation unavailable during revert"
            );
            None
        }
    };
    let mut source_meta = codex_rollout::read_session_meta_line(source_path.as_path())
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!(
                "failed to read current paginated rollout {}: {err}",
                source_path.display()
            ),
        })?
        .meta;
    if source_meta.id != current_rollout.thread_id {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!("current rollout for {thread_id} belongs to another thread"),
        });
    }
    if source_meta.history_mode != ThreadHistoryMode::Paginated {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!("thread {thread_id} does not use paginated history"),
        });
    }

    // Older binaries can omit creator fields when replacing a rollout during revert.
    source_meta.creator_user_id = source_meta
        .creator_user_id
        .or(stored_metadata.creator_user_id);
    source_meta.creator_account_id = source_meta
        .creator_account_id
        .or(stored_metadata.creator_account_id);

    // Preserve old-reader compatibility when introducing the first reference to a standalone
    // source. Already-shared ancestors stay read-only; their offsets address decoded JSONL bytes.
    let mut lineage = store
        .resolve_rollout_lineage(thread_id, /*initial_path*/ None)
        .await?;
    for segment in &mut lineage.segments {
        if segment.rollout_id() == current_rollout.rollout_id && source_meta.history_base.is_none()
        {
            segment.rollout_path =
                codex_rollout::materialize_rollout_for_reference(segment.rollout_path.as_path())
                    .await
                    .map_err(|err| ThreadStoreError::Internal {
                        message: format!(
                            "failed to materialize rollout {} for revert: {err}",
                            segment.rollout_path.display()
                        ),
                    })?;
        }
        super::thread_history_materialization::materialize_to_sqlite(
            store,
            segment.rollout_id(),
            segment.rollout_path.as_path(),
        )
        .await?;
    }
    let history_base = paginated_fork::history_base_at_boundary(
        store,
        thread_id,
        ForkBoundary::BeforeTurn(before_turn_id),
        &lineage,
    )
    .await?;

    let forked_from_ordinal_exclusive =
        codex_rollout::forked_from_ordinal_exclusive(&source_meta, Some(source_path.as_path()))
            .map(|cutoff| {
                // Reverting into inherited history can shrink, but never grow, the parent prefix.
                cutoff.min(history_base.map_or(0, |base| base.end_ordinal_exclusive))
            });

    source_meta.multi_agent_version = multi_agent_version.or(source_meta.multi_agent_version);
    let rollout_id = ThreadId::new();
    let recorder = create_replacement_recorder(
        store,
        source_meta,
        rollout_id,
        history_base,
        forked_from_ordinal_exclusive,
        writer_lock,
        preserved_receipts.is_some(),
    )
    .await?;
    let recorder = prepare_replacement(recorder, preserved_receipts).await?;
    let replacement_path = recorder.rollout_path().to_path_buf();
    publish_replacement(
        &state_db,
        thread_id,
        expected_sqlite_path.as_path(),
        replacement_path.as_path(),
    )
    .await
}

/// Own the unpublished file until its complete receipt copy is closed. Cleanup
/// drains the writer before unlinking, retaining its cross-process fence.
struct UnpublishedReplacement {
    recorder: Option<RolloutRecorder>,
    runtime: tokio::runtime::Handle,
}

impl UnpublishedReplacement {
    fn new(recorder: RolloutRecorder) -> Self {
        Self {
            recorder: Some(recorder),
            runtime: tokio::runtime::Handle::current(),
        }
    }

    fn retire(&mut self) -> Option<tokio::task::JoinHandle<()>> {
        let recorder = self.recorder.take()?;
        Some(self.runtime.spawn(async move {
            let path = recorder.rollout_path();
            if let Err(error) = recorder.discard().await {
                tracing::warn!(path = %path.display(), %error, "failed to drain unpublished replacement");
            }
            if let Err(error) = tokio::fs::remove_file(path).await
                && error.kind() != std::io::ErrorKind::NotFound
            {
                tracing::warn!(path = %path.display(), %error, "failed to remove unpublished replacement");
            }
        }))
    }

    fn into_closed(mut self) -> RolloutRecorder {
        self.recorder.take().expect("replacement still owned")
    }
}

impl Drop for UnpublishedReplacement {
    fn drop(&mut self) {
        // A cancelled cleanup await detaches this bounded task; the recorder and
        // fence remain owned until already-running IO has drained and removal ends.
        let _ = self.retire();
    }
}

async fn prepare_replacement(
    recorder: RolloutRecorder,
    preserved_receipts: Option<Vec<codex_protocol::attention::ThreadAttentionNotification>>,
) -> ThreadStoreResult<RolloutRecorder> {
    let mut replacement = UnpublishedReplacement::new(recorder);
    let recorder = replacement
        .recorder
        .as_ref()
        .expect("replacement still owned");
    let result = async {
        recorder.persist().await.map_err(thread_store_io_error)?;
        stage_replacement_receipts(recorder, preserved_receipts).await
    }
    .await;
    if let Err(error) = result {
        if let Some(cleanup) = replacement.retire()
            && let Err(cleanup_error) = cleanup.await
        {
            tracing::warn!(%cleanup_error, "unpublished replacement cleanup task failed");
        }
        return Err(error);
    }
    // Disarm BEFORE submitting CAS: a cancelled SQL await can still commit.
    // This closed file already contains the complete captured receipt state.
    Ok(replacement.into_closed())
}

/// Called only after the staged header has been materialized. Receipt write and
/// close must succeed before the pointer can be published by the caller.
async fn stage_replacement_receipts(
    recorder: &RolloutRecorder,
    preserved_receipts: Option<Vec<codex_protocol::attention::ThreadAttentionNotification>>,
) -> ThreadStoreResult<()> {
    if let Some(notices) = preserved_receipts {
        let items: Vec<_> = notices
            .into_iter()
            .map(|notice| {
                codex_rollout::RolloutItem::EventMsg(
                    codex_protocol::protocol::EventMsg::ThreadAttention(notice),
                )
            })
            .collect();
        recorder
            .record_canonical_items(&items)
            .await
            .map_err(thread_store_io_error)?;
        recorder.flush().await.map_err(thread_store_io_error)?;
    }
    recorder.shutdown().await.map_err(thread_store_io_error)
}

async fn publish_replacement(
    state_db: &codex_state::StateRuntime,
    thread_id: ThreadId,
    expected_sqlite_path: &std::path::Path,
    replacement_path: &std::path::Path,
) -> ThreadStoreResult<()> {
    let replaced = state_db
        .replace_rollout_path_if_current(thread_id, expected_sqlite_path, replacement_path)
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to switch thread {thread_id} to reverted rollout: {err}"),
        })?;
    if !replaced {
        let _ = tokio::fs::remove_file(replacement_path).await;
        return Err(ThreadStoreError::Conflict {
            message: format!("thread {thread_id} changed while it was being reverted"),
        });
    }
    Ok(())
}

async fn create_replacement_recorder(
    store: &LocalThreadStore,
    source_meta: codex_rollout::SessionMeta,
    rollout_id: ThreadId,
    history_base: Option<codex_protocol::protocol::HistoryPosition>,
    forked_from_ordinal_exclusive: Option<u64>,
    writer_lock: super::WriterLockGuard,
    receipt_lifetime_proved: bool,
) -> ThreadStoreResult<RolloutRecorder> {
    let config = RolloutConfig {
        codex_home: store.config.codex_home.clone(),
        sqlite: store.config.sqlite.clone(),
        cwd: source_meta.cwd.clone(),
        model_provider_id: source_meta
            .model_provider
            .clone()
            .unwrap_or_else(|| store.config.default_model_provider_id.clone()),
        generate_memories: source_meta.memory_mode.as_deref() != Some("disabled"),
    };
    let mut params = RolloutRecorderParams::new(
        source_meta.id,
        source_meta.forked_from_id,
        source_meta.parent_thread_id,
        source_meta.source,
        source_meta.thread_source,
        source_meta.originator,
        source_meta.base_instructions.unwrap_or_default(),
        source_meta.dynamic_tools.unwrap_or_default(),
    )
    .with_creator(source_meta.creator_user_id, source_meta.creator_account_id)
    .with_session_id(source_meta.session_id)
    .with_rollout_id(rollout_id)
    .with_selected_capability_roots(source_meta.selected_capability_roots)
    .with_runtime_workspace_roots(source_meta.runtime_workspace_roots)
    .with_multi_agent_version(source_meta.multi_agent_version)
    .with_attention_receipt_lifetime_version(receipt_lifetime_proved.then_some(1))
    .with_history_mode(ThreadHistoryMode::Paginated)
    .with_history_base(history_base)
    .with_forked_from_ordinal_exclusive(forked_from_ordinal_exclusive)
    .with_subagent_history_start_ordinal(source_meta.subagent_history_start_ordinal);
    if let Some(context_window) = source_meta.context_window {
        params = params.with_initial_window_id(context_window.window_id);
    }
    RolloutRecorder::new_with_writer_lock(&config, params, writer_lock)
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to create reverted rollout: {err}"),
        })
}

fn thread_store_io_error(err: std::io::Error) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: err.to_string(),
    }
}

#[cfg(test)]
#[path = "revert_thread_tests.rs"]
mod tests;
