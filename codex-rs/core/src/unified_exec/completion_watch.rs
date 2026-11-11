//! Hand-armed completion subscriptions for existing sandboxed exec processes.
//!
//! A subscription is local to this live thread. It survives ordinary turn finish,
//! but not interrupt, task replacement, compaction or shutdown. It is never
//! reconstructed from command output or replayed blindly after a process restart.
use super::UnifiedExecContext;
use super::UnifiedExecProcess;
use super::UnifiedExecProcessManager;
use codex_protocol::attention::ThreadAttentionResponse;
use codex_protocol::models::FunctionCallOutputPayload;
use codex_protocol::models::ResponseItem;
use codex_protocol::turn_input::TurnInput;
use codex_protocol::turn_input::TurnInputRequest;
use codex_utils_string::truncate_middle_chars;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub(super) struct CompletionWatches {
    generation: u64,
    watches: HashMap<i32, Arc<CompletionWatch>>,
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::session::session::Session;
    use crate::session::step_context::StepContext;
    use crate::session::tests::make_session_and_context;
    use crate::session::turn_context::TurnContext;
    use crate::state::TaskKind;
    use crate::tasks::SessionTask;
    use crate::tasks::SessionTaskResult;
    use crate::tools::context::ToolCallSource;
    use crate::tools::context::ToolInvocation;
    use crate::tools::context::ToolPayload;
    use crate::tools::handlers::ExecCommandHandler;
    use crate::tools::registry::ToolExecutor;
    use crate::turn_diff_tracker::TurnDiffTracker;
    use codex_protocol::protocol::TurnAbortReason;
    use std::time::Duration;
    use tokio::sync::Mutex;

    struct RunningTask;
    impl SessionTask for RunningTask {
        fn kind(&self) -> TaskKind {
            TaskKind::Regular
        }
        fn span_name(&self) -> &'static str {
            "completion_watch_test"
        }
        async fn run(
            self: Arc<Self>,
            _session: Arc<Session>,
            _ctx: Arc<TurnContext>,
            _input: Vec<crate::session::TurnInput>,
            token: CancellationToken,
        ) -> SessionTaskResult {
            token.cancelled().await;
            Ok(None)
        }
    }

    async fn launch(session: &Arc<Session>, turn: &Arc<TurnContext>, cmd: &str, notify: bool) {
        let result = ExecCommandHandler::default()
            .handle(ToolInvocation {
                session: Arc::clone(session),
                step_context: StepContext::for_test(Arc::clone(turn)),
                turn: Arc::clone(turn),
                cancellation_token: CancellationToken::new(),
                tracker: Arc::new(Mutex::new(TurnDiffTracker::new())),
                call_id: "watch-call".into(),
                tool_name: codex_tools::ToolName::plain("exec_command"),
                source: ToolCallSource::Direct,
                native_call: None,
                environments: Default::default(),
                payload: ToolPayload::Function {
                    arguments: serde_json::json!({
                        "cmd": cmd, "shell": "/bin/bash", "login": false,
                        "yield_time_ms": 250, "notify_on_exit": notify,
                    })
                    .to_string(),
                },
            })
            .await;
        assert!(result.is_ok(), "watch command failed: {:?}", result.err());
    }

    #[tokio::test]
    async fn completion_watch_is_opt_in_and_does_not_duplicate_inline_completion() {
        let (session, turn) = make_session_and_context().await;
        let session = Arc::new(session);
        let turn = Arc::new(turn);
        launch(&session, &turn, "sleep 1", false).await;
        assert!(
            session
                .services
                .unified_exec_manager
                .completion_watches
                .lock()
                .await
                .watches
                .is_empty()
        );
        launch(&session, &turn, "printf inline", true).await;
        assert!(
            session
                .services
                .unified_exec_manager
                .completion_watches
                .lock()
                .await
                .watches
                .is_empty()
        );
        session.close_unified_exec_processes().await;
    }

    #[tokio::test]
    async fn completion_watch_preserves_question_and_delivers_once_without_steering() {
        let (session, turn) = make_session_and_context().await;
        let session = Arc::new(session);
        let turn = Arc::new(turn);
        session
            .spawn_task(Arc::clone(&turn), Vec::new(), RunningTask)
            .await;
        let state = session
            .active_turn
            .lock()
            .await
            .as_ref()
            .unwrap()
            .turn_state
            .clone();
        let (activity, _) = session.input_queue.subscribe_activity(Some(&state)).await;
        let (answer_tx, answer_rx) = tokio::sync::oneshot::channel();
        state
            .lock()
            .await
            .insert_pending_user_input("question".into(), answer_tx);
        launch(&session, &turn, "sleep 0.8; printf watched-result", true).await;
        tokio::time::sleep(Duration::from_millis(1000)).await;
        assert!(
            !session
                .input_queue
                .has_pending_input(&session.active_turn)
                .await
        );
        assert_eq!(
            session
                .services
                .unified_exec_manager
                .completion_watches
                .lock()
                .await
                .watches
                .len(),
            1
        );
        let callback = state
            .lock()
            .await
            .remove_pending_user_input("question")
            .unwrap();
        assert!(
            !callback.is_closed(),
            "the watcher consumed the question callback"
        );
        drop(callback);
        assert!(answer_rx.await.is_err());
        tokio::time::timeout(Duration::from_secs(5), async {
            while !session
                .input_queue
                .has_pending_input(&session.active_turn)
                .await
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let pending = session
            .input_queue
            .take_pending_input_for_turn_state(&state)
            .await;
        assert_eq!(pending.len(), 1);
        let crate::session::TurnInput::ResponseItem(envelope) = &pending[0] else {
            panic!("not tool evidence")
        };
        let wire = serde_json::to_value(&envelope.item).unwrap();
        assert_eq!(wire["type"], "function_call_output");
        assert_eq!(wire["name"], "exec_command_completion");
        assert!(wire["call_id"].is_null());
        assert!(wire.get("role").is_none());
        let output: serde_json::Value =
            serde_json::from_str(wire["output"].as_str().unwrap()).unwrap();
        assert_eq!(output["output"], "watched-result");
        assert_eq!(output["originating_task_id"], turn.sub_id);
        assert_eq!(output["originating_call_id"], "watch-call");
        assert_eq!(output["exit_code"], 0);
        assert!(
            !activity.has_changed().unwrap(),
            "completion steered the active task"
        );
        assert!(
            session
                .services
                .unified_exec_manager
                .completion_watches
                .lock()
                .await
                .watches
                .is_empty()
        );
        assert!(
            !session
                .active_turn
                .lock()
                .await
                .as_ref()
                .unwrap()
                .task
                .as_ref()
                .unwrap()
                .cancellation_token
                .is_cancelled()
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            !session
                .input_queue
                .has_pending_input(&session.active_turn)
                .await
        );
        session.abort_all_tasks(TurnAbortReason::Interrupted).await;
        session.close_unified_exec_processes().await;
    }

    #[tokio::test]
    async fn completion_watch_interrupt_cancels_only_watched_processes_and_advances_generation() {
        let (session, turn) = make_session_and_context().await;
        let session = Arc::new(session);
        let turn = Arc::new(turn);
        session
            .spawn_task(Arc::clone(&turn), Vec::new(), RunningTask)
            .await;
        launch(&session, &turn, "sleep 30", false).await;
        launch(&session, &turn, "sleep 30", true).await;
        let manager = &session.services.unified_exec_manager;
        let generation = manager.completion_watch_generation().await;
        let watched = manager
            .completion_watches
            .lock()
            .await
            .watches
            .values()
            .next()
            .unwrap()
            .clone();
        session.abort_all_tasks(TurnAbortReason::Interrupted).await;
        assert!(watched.cancelled.is_cancelled());
        assert!(manager.completion_watch_generation().await > generation);
        assert!(manager.completion_watches.lock().await.watches.is_empty());
        tokio::time::timeout(
            Duration::from_secs(3),
            watched.process.cancellation_token().cancelled(),
        )
        .await
        .unwrap();
        assert_eq!(
            session.list_background_terminals().await.len(),
            1,
            "ordinary background terminal was terminated"
        );
        assert!(
            !session
                .input_queue
                .has_pending_input(&session.active_turn)
                .await
        );
        session.close_unified_exec_processes().await;
    }
}

struct CompletionWatch {
    process: Arc<UnifiedExecProcess>,
    cancelled: CancellationToken,
}

fn notification_item(
    thread_id: String,
    task_id: String,
    call_id: String,
    process_id: i32,
    process: &UnifiedExecProcess,
    output: String,
) -> ResponseItem {
    ResponseItem::FunctionCallOutput {
        id: None,
        call_id: None,
        name: Some("exec_command_completion".into()),
        namespace: Some("observatory".into()),
        output: FunctionCallOutputPayload::from_text(
            serde_json::json!({
                "type": "background_command_completion",
                "thread_id": thread_id,
                "originating_task_id": task_id,
                "originating_call_id": call_id,
                "session_id": process_id,
                "exit_code": process.exit_code(),
                "timed_out": process.timed_out(),
                "error": process.failure_message(),
                "output": truncate_middle_chars(&output, 4096),
                "output_truncated": output.chars().count() > 4096,
            })
            .to_string(),
        ),
        internal_chat_message_metadata_passthrough: None,
    }
}

impl UnifiedExecProcessManager {
    pub(crate) async fn completion_watch_generation(&self) -> u64 {
        self.completion_watches.lock().await.generation
    }

    pub(crate) async fn arm_completion_watch(
        &self,
        process_id: i32,
        context: &UnifiedExecContext,
        generation: u64,
    ) -> Result<(), String> {
        let mut watches = self.completion_watches.lock().await;
        let store = self.process_store.lock().await;
        let entry = store
            .processes
            .get(&process_id)
            .ok_or_else(|| "background process is no longer available for watching".to_string())?;
        if generation != watches.generation || context.cancellation_token.is_cancelled() {
            entry.process.terminate();
            return Err("completion watcher was cancelled before it could be armed".into());
        }
        let watch = Arc::new(CompletionWatch {
            process: Arc::clone(&entry.process),
            cancelled: CancellationToken::new(),
        });
        let event_done = entry.completion_event_done.clone();
        if watches.watches.contains_key(&process_id) {
            return Err("a completion watcher is already armed for this process".into());
        }
        watches.watches.insert(process_id, Arc::clone(&watch));
        drop(store);
        drop(watches);
        let session = Arc::downgrade(&context.session);
        let task_id = context.step_context.turn.sub_id.clone();
        let call_id = context.call_id.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = watch.cancelled.cancelled() => return,
                _ = event_done.cancelled() => {}
            }
            let Some(session) = session.upgrade() else {
                return;
            };
            // The existing exit watcher records the completed execution first.
            // Do not wake a model if its durable evidence could not be flushed.
            if let Err(error) = session.flush_rollout().await {
                tracing::warn!(%error, process_id, "completion notification held: rollout flush failed");
                return;
            }
            let output = {
                let buffer = watch.process.output_handles().output_buffer.lock().await;
                String::from_utf8_lossy(&buffer.transcript.to_bytes_with_omission_marker())
                    .into_owned()
            };
            let item = notification_item(
                session.thread_id.to_string(),
                task_id,
                call_id,
                process_id,
                &watch.process,
                output,
            );
            loop {
                // Serialize delivery against cancellation. Admission itself owns
                // the active/idle seam and cannot steer or consume question replies.
                let mut registry = session
                    .services
                    .unified_exec_manager
                    .completion_watches
                    .lock()
                    .await;
                if !registry
                    .watches
                    .get(&process_id)
                    .is_some_and(|current| Arc::ptr_eq(current, &watch))
                    || watch.cancelled.is_cancelled()
                {
                    return;
                }
                if session.is_interrupted() {
                    registry.watches.remove(&process_id);
                    return;
                }
                let mut request = TurnInputRequest::new(TurnInput::ResponseItem(item.clone()));
                request.background_attention = true;
                request.start.turn_trigger = Some("background_command_completion".into());
                let result =
                    crate::attention::admit(&session, request, uuid::Uuid::now_v7().to_string())
                        .await;
                match result {
                    Ok(ThreadAttentionResponse::Accepted { .. }) => {
                        registry.watches.remove(&process_id);
                        return;
                    }
                    Ok(ThreadAttentionResponse::Held { .. }) => {}
                    Err(error) => {
                        // An uncertain admission is not safe to replay. The
                        // execution event remains inspectable in this thread.
                        registry.watches.remove(&process_id);
                        tracing::warn!(%error, process_id, "completion notification admission failed; not replaying");
                        return;
                    }
                }
                drop(registry);
                tokio::select! {
                    _ = watch.cancelled.cancelled() => return,
                    _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => {}
                }
            }
        });
        Ok(())
    }

    pub(crate) async fn cancel_completion_watch(&self, process_id: i32) {
        if let Some(watch) = self
            .completion_watches
            .lock()
            .await
            .watches
            .remove(&process_id)
        {
            watch.cancelled.cancel();
        }
    }

    pub(crate) async fn cancel_completion_watches(&self) {
        let watches = {
            let mut registry = self.completion_watches.lock().await;
            registry.generation = registry.generation.wrapping_add(1);
            registry
                .watches
                .drain()
                .map(|(_, watch)| {
                    watch.cancelled.cancel();
                    watch
                })
                .collect::<Vec<_>>()
        };
        // Only opted-in watcher commands are terminated here. Ordinary background
        // terminals keep the existing process manager's lifetime and permissions.
        for watch in watches {
            watch.process.terminate();
        }
    }
}
