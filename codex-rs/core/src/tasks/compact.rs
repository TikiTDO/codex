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
        _cancellation_token: CancellationToken,
    ) -> SessionTaskResult {
        let _profile_guard = ctx.turn_timing_state.begin_compaction();
        if ctx.config.features.enabled(Feature::TokenBudget) {
            crate::compact_token_budget::run_manual_compact_task(session, ctx).await?;
            return Ok(None);
        }

        let result = match ctx.provider.capabilities().remote_compaction {
            RemoteCompactionSupport::V2
                if ctx.config.features.enabled(Feature::RemoteCompactionV2) =>
            {
                emit_compact_metric(
                    &session.services.session_telemetry,
                    "remote_v2",
                    /*manual*/ true,
                );
                crate::compact_remote_v2::run_remote_compact_task(
                    session.clone(),
                    ctx,
                    self.input.as_ref(),
                )
                .await
            }
            RemoteCompactionSupport::V2 => {
                emit_compact_metric(
                    &session.services.session_telemetry,
                    "remote",
                    /*manual*/ true,
                );
                crate::compact_remote::run_remote_compact_task(
                    session.clone(),
                    ctx,
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
                    text: crate::compact::apply_compaction_prompt_instructions(
                        ctx.config
                            .compact_prompt
                            .as_deref()
                            .unwrap_or(crate::compact::SUMMARIZATION_PROMPT)
                            .to_string(),
                        self.input.as_ref(),
                    ),
                    // Compaction prompt is synthesized; no UI element ranges to preserve.
                    text_elements: Vec::new(),
                }];
                crate::compact::run_compact_task(session.clone(), ctx, input, self.input.as_ref())
                    .await
            }
        };
        let succeeded = result.is_ok();
        if let Err(err) = result
            && matches!(err.details(), CodexErrorDetails::TurnAborted)
        {
            return Err(err);
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
