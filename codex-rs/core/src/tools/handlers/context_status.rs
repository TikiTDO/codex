use crate::function_tool::FunctionCallError;
use crate::session::context_window::context_window_token_status;
use crate::state::TaskKind;
use crate::tools::context::FunctionToolOutput;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolOutput;
use crate::tools::context::ToolPayload;
use crate::tools::context::boxed_tool_output;
use crate::tools::handlers::context_status_spec::CONTEXT_STATUS_TOOL_NAME;
use crate::tools::handlers::context_status_spec::create_context_status_tool;
use crate::tools::registry::CoreToolRuntime;
use crate::tools::registry::ToolExecutor;
use codex_protocol::models::ResponseInputItem;
use codex_protocol::protocol::TokenUsage;
use codex_tools::ToolName;
use codex_tools::ToolSpec;
use serde::Serialize;
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, Serialize)]
struct ContextStatusOutput {
    model: String,
    model_provider: String,
    reasoning_effort: String,
    thread_id: String,
    turn_id: String,
    turn_active: bool,
    turn_kind: Option<&'static str>,
    context_tokens_used: i64,
    context_window_tokens: Option<i64>,
    context_tokens_remaining: Option<i64>,
    context_window_percent_used: Option<f64>,
    auto_compact_tokens_used: i64,
    auto_compact_limit: Option<i64>,
    auto_compact_tokens_remaining: Option<i64>,
    session_total_usage: Option<TokenUsage>,
    last_model_usage: Option<TokenUsage>,
}

impl ContextStatusOutput {
    fn rendered(&self) -> String {
        serde_json::to_string(self)
            .unwrap_or_else(|err| format!("failed to serialize context status tool output: {err}"))
    }
}

impl ToolOutput for ContextStatusOutput {
    fn log_preview(&self) -> String {
        self.rendered()
    }

    fn success_for_logging(&self) -> bool {
        true
    }

    fn to_response_item(&self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
        FunctionToolOutput::from_text(self.rendered(), Some(true))
            .to_response_item(call_id, payload)
    }

    fn code_mode_result(&self, _payload: &ToolPayload) -> JsonValue {
        serde_json::to_value(self).unwrap_or_else(|err| {
            JsonValue::String(format!(
                "failed to serialize context status tool output: {err}"
            ))
        })
    }
}

pub struct ContextStatusHandler;

impl ToolExecutor<ToolInvocation> for ContextStatusHandler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain(CONTEXT_STATUS_TOOL_NAME)
    }

    fn spec(&self) -> ToolSpec {
        create_context_status_tool()
    }

    fn handle(&self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'_> {
        Box::pin(async move {
            if !matches!(invocation.payload, ToolPayload::Function { .. }) {
                return Err(FunctionCallError::RespondToModel(
                    "context_status handler received unsupported payload".to_string(),
                ));
            }

            let token_status =
                context_window_token_status(invocation.session.as_ref(), invocation.turn.as_ref())
                    .await;
            let usage = invocation.session.token_usage_info().await;
            let (turn_active, turn_kind) = {
                let active = invocation.session.active_turn.lock().await;
                active
                    .as_ref()
                    .and_then(|active_turn| active_turn.task.as_ref())
                    .map_or((false, None), |task| {
                        let active = task.turn_context.sub_id == invocation.turn.sub_id;
                        let kind = match task.kind {
                            TaskKind::Regular => "regular",
                            TaskKind::Review => "review",
                            TaskKind::Compact => "compact",
                        };
                        (active, Some(kind))
                    })
            };
            let context_tokens_remaining = token_status.full_context_window_limit.map(|limit| {
                limit
                    .saturating_sub(token_status.active_context_tokens)
                    .max(0)
            });
            let context_window_percent_used = token_status
                .full_context_window_limit
                .filter(|limit| *limit > 0)
                .map(|limit| token_status.active_context_tokens as f64 * 100.0 / limit as f64);
            let auto_compact_tokens_remaining =
                token_status.auto_compact_scope_limit.map(|limit| {
                    limit
                        .saturating_sub(token_status.auto_compact_scope_tokens)
                        .max(0)
                });

            Ok(boxed_tool_output(ContextStatusOutput {
                model: invocation.turn.model_info.slug.clone(),
                model_provider: invocation.turn.config.model_provider_id.clone(),
                reasoning_effort: invocation.turn.effective_reasoning_effort_for_tracing(),
                thread_id: invocation.session.thread_id.to_string(),
                turn_id: invocation.turn.sub_id.clone(),
                turn_active,
                turn_kind,
                context_tokens_used: token_status.active_context_tokens,
                context_window_tokens: token_status.full_context_window_limit,
                context_tokens_remaining,
                context_window_percent_used,
                auto_compact_tokens_used: token_status.auto_compact_scope_tokens,
                auto_compact_limit: token_status.auto_compact_scope_limit,
                auto_compact_tokens_remaining,
                session_total_usage: usage.as_ref().map(|usage| usage.total_token_usage.clone()),
                last_model_usage: usage.map(|usage| usage.last_token_usage),
            }))
        })
    }
}

impl CoreToolRuntime for ContextStatusHandler {}
