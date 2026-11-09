use std::sync::Arc;

use super::SessionTask;
use super::SessionTaskResult;
use super::emit_compact_metric;
use crate::session::TurnInput;
use crate::session::session::Session;
use crate::session::turn_context::TurnContext;
use crate::state::TaskKind;
use codex_features::Feature;
use codex_model_provider::RemoteCompactionSupport;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::protocol::CodexErrorInfo;
use codex_protocol::protocol::CompactionInput;
use codex_protocol::user_input::UserInput;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Default)]
pub(crate) struct CompactTask {
    input: Option<CompactionInput>,
}

impl CompactTask {
    pub(crate) fn new(input: Option<CompactionInput>) -> Self {
        Self { input }
    }
}

impl SessionTask for CompactTask {
    fn kind(&self) -> TaskKind {
        TaskKind::Compact
    }

    fn span_name(&self) -> &'static str {
        "session_task.compact"
    }

    async fn run(
        self: Arc<Self>,
        session: Arc<Session>,
        ctx: Arc<TurnContext>,
        _input: Vec<TurnInput>,
        cancellation_token: CancellationToken,
    ) -> SessionTaskResult {
        let _profile_guard = ctx.turn_timing_state.begin_compaction();
        let _compaction_span = tracing::trace_span!(
            "codex.compaction",
            codex.turn.phase = "compaction",
            conversation.id = %session.thread_id,
            turn.id = %ctx.sub_id,
        );
        // Preparation errors must reach the task runner, which reports them to the client.
        session.emit_turn_started(&ctx, TaskKind::Compact).await;
        let step_context = session
            .capture_step_context(Arc::clone(&ctx), &cancellation_token)
            .await?;
        let world_state = Arc::new(
            session
                .build_world_state_for_step(&step_context, /*new_window*/ true)
                .await?,
        );
        if crate::compact::uses_native_token_budget_reset(
            ctx.config.features.enabled(Feature::TokenBudget),
            self.input.as_ref(),
        ) {
            crate::compact_token_budget::run_manual_compact_task(
                Arc::clone(&session),
                step_context,
                world_state,
            )
            .await?;
            if self
                .input
                .as_ref()
                .is_some_and(|input| input.retry_websocket)
            {
                session.services.model_client.retry_websocket();
            }
            return Ok(None);
        }

        let result = match ctx.provider.capabilities().remote_compaction {
            RemoteCompactionSupport::V2 => {
                emit_compact_metric(
                    &session.services.session_telemetry,
                    "remote_v2",
                    /*manual*/ true,
                );
                crate::compact_remote_v2::run_remote_compact_task_with_input(
                    session.clone(),
                    step_context,
                    world_state,
                    self.input.as_ref(),
                )
                .await
            }
            RemoteCompactionSupport::Unsupported => {
                emit_compact_metric(
                    &session.services.session_telemetry,
                    "local",
                    /*manual*/ true,
                );
                let input = vec![UserInput::Text {
                    text: ctx
                        .config
                        .compact_prompt
                        .as_deref()
                        .unwrap_or(crate::compact::SUMMARIZATION_PROMPT)
                        .to_string(),
                    // Compaction prompt is synthesized; no UI element ranges to preserve.
                    text_elements: Vec::new(),
                }];
                crate::compact::run_compact_task_with_input(
                    session.clone(),
                    step_context,
                    world_state,
                    input,
                    self.input.as_ref(),
                )
                .await
            }
        };
        let succeeded = result.is_ok();
        if let Err(err) = result {
            if matches!(err.details(), CodexErrorDetails::TurnAborted) {
                return Err(err);
            }
            let error = err.to_codex_protocol_error();
            if matches!(error, CodexErrorInfo::UsageLimitExceeded) {
                // Compaction already emitted the error; notify extensions without emitting it twice.
                session
                    .emit_turn_error_lifecycle(ctx.as_ref(), error, err.details())
                    .await;
            }
        }
        if succeeded
            && self
                .input
                .as_ref()
                .is_some_and(|input| input.retry_websocket)
        {
            session.services.model_client.retry_websocket();
        }
        Ok(None)
    }
}

#[cfg(test)]
mod estate_tests {
    use super::*;
    use crate::session::tests::make_session_and_context_with_auth_and_config_and_rx;
    use codex_login::CodexAuth;
    use codex_protocol::models::ResponseItem;

    #[test_case::test_case(false; "default preserves sticky fallback")]
    #[test_case::test_case(true; "retry only reopens after native reset")]
    #[tokio::test]
    async fn estate_compaction_token_budget_default_and_retry_only_use_native_reset(retry: bool) {
        let (session, turn, _events) = make_session_and_context_with_auth_and_config_and_rx(
            CodexAuth::create_dummy_chatgpt_auth_for_testing(),
            Vec::new(),
            |config| {
                config.features.enable(Feature::TokenBudget).unwrap();
                config.model_provider.supports_websockets = true;
            },
        )
        .await;
        session
            .replace_history(
                vec![
                    serde_json::from_value::<ResponseItem>(serde_json::json!({
                        "type": "message", "role": "user",
                        "content": [{"type": "input_text", "text": "NATIVE_RESET_OLD_CONTEXT"}]
                    }))
                    .unwrap(),
                ],
                None,
            )
            .await;
        let client = &session.services.model_client;
        assert!(client.force_http_fallback(&turn.session_telemetry, turn.model_info()));
        let input = retry.then_some(CompactionInput {
            retry_websocket: true,
            ..Default::default()
        });
        Arc::new(CompactTask::new(input))
            .run(
                Arc::clone(&session),
                turn,
                Vec::new(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(client.responses_websocket_enabled(), retry);
        assert!(
            !format!("{:?}", session.clone_history().await).contains("NATIVE_RESET_OLD_CONTEXT")
        );
    }

    #[tokio::test]
    async fn estate_compaction_failed_remote_request_never_reopens_sticky_transport() {
        let server = core_test_support::responses::start_mock_server().await;
        let requests = core_test_support::responses::mount_sse_once(
            &server,
            core_test_support::responses::sse_failed(
                "failed-compact",
                "usage_limit_reached",
                "bounded fixture failure",
            ),
        )
        .await;
        let uri = format!("{}/v1", server.uri());
        let (session, turn, _events) = make_session_and_context_with_auth_and_config_and_rx(
            CodexAuth::create_dummy_chatgpt_auth_for_testing(),
            Vec::new(),
            move |config| {
                config.features.disable(Feature::TokenBudget).unwrap();
                config.model_provider.base_url = Some(uri);
                config.model_provider.supports_websockets = true;
            },
        )
        .await;
        let client = &session.services.model_client;
        assert!(client.force_http_fallback(&turn.session_telemetry, turn.model_info()));
        let task = CompactTask::new(Some(CompactionInput {
            retry_websocket: true,
            ..Default::default()
        }));
        Arc::new(task)
            .run(
                Arc::clone(&session),
                turn,
                Vec::new(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert!(!client.responses_websocket_enabled());
        assert_eq!(requests.requests().len(), 1);
    }

    #[test_case::test_case(false; "pre hook stop")]
    #[test_case::test_case(true; "post hook stop")]
    #[tokio::test]
    async fn estate_compaction_stopped_hook_never_reopens_sticky_transport(post: bool) {
        use core_test_support::responses;
        let server = responses::start_mock_server().await;
        let requests = responses::mount_sse_once(
            &server,
            responses::sse(vec![
                serde_json::json!({"type":"response.output_item.done", "item": {
                    "type":"compaction", "encrypted_content":"stopped-summary"
                }}),
                responses::ev_completed("stopped-compaction"),
            ]),
        )
        .await;
        let uri = format!("{}/v1", server.uri());
        let (session, turn, events) = make_session_and_context_with_auth_and_config_and_rx(
            CodexAuth::create_dummy_chatgpt_auth_for_testing(),
            Vec::new(),
            move |config| {
                config.features.disable(Feature::TokenBudget).unwrap();
                config.model_provider.base_url = Some(uri);
                config.model_provider.supports_websockets = true;
            },
        )
        .await;
        let root = tempfile::tempdir().unwrap();
        let root = codex_utils_absolute_path::AbsolutePathBuf::try_from(root.path()).unwrap();
        let event = if post { "PostCompact" } else { "PreCompact" };
        let hooks: codex_config::HookEventsToml = serde_json::from_value(serde_json::json!({
            event: [{"hooks": [{"type":"command", "command":
                "printf '%s\\n' '{\"continue\":false,\"stopReason\":\"bounded fixture stop\"}'"
            }]}]
        }))
        .unwrap();
        let (hooks, _) = codex_hooks::Hooks::new(
            codex_hooks::HooksConfig {
                feature_enabled: true,
                bypass_hook_trust: true,
                shell_program: Some("/bin/sh".into()),
                shell_args: vec!["-c".into()],
                plugin_hook_sources: vec![codex_plugin::PluginHookSource {
                    plugin_id: codex_plugin::PluginId::parse("compact-stop@test").unwrap(),
                    plugin_root: root.clone(),
                    plugin_data_root: root.join("data"),
                    source_path: root.join("hooks.json"),
                    source_relative_path: "hooks.json".into(),
                    hooks,
                }],
                ..Default::default()
            },
            session.thread_id,
            Arc::new(crate::hook_mcp_executor::CoreHookMcpExecutor {
                runtime: Arc::clone(&session.services.mcp_runtime),
                thread_id: session.thread_id,
            }),
        )
        .unwrap();
        session.services.hooks.store(Arc::new(hooks));
        let client = &session.services.model_client;
        assert!(client.force_http_fallback(&turn.session_telemetry, turn.model_info()));
        let result = Arc::new(CompactTask::new(Some(CompactionInput {
            retry_websocket: true,
            ..Default::default()
        })))
        .run(
            Arc::clone(&session),
            turn,
            Vec::new(),
            CancellationToken::new(),
        )
        .await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err().details(),
            CodexErrorDetails::TurnAborted
        ));
        assert!(!client.responses_websocket_enabled());
        assert_eq!(requests.requests().len(), usize::from(post));
        assert!(
            std::iter::from_fn(|| events.try_recv().ok()).any(|event| {
                matches!(
                    event.msg,
                    codex_protocol::protocol::EventMsg::HookCompleted(_)
                )
            }),
            "the real hook must execute, not just fail compaction setup"
        );
    }
}
