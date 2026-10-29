use crate::function_tool::FunctionCallError;
use crate::session::context_window::context_window_token_status_for_model;
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
    fn log_output(&self) -> String {
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

    fn handle<'a>(&'a self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'a>
    where
        ToolInvocation: 'a,
    {
        Box::pin(async move {
            if !matches!(invocation.payload, ToolPayload::Function { .. }) {
                return Err(FunctionCallError::RespondToModel(
                    "context_status handler received unsupported payload".to_string(),
                ));
            }

            let step = &invocation.step_context;
            let token_status = context_window_token_status_for_model(
                invocation.session.as_ref(),
                step.turn.config.as_ref(),
                step.turn.as_ref(),
                step.settings.model_info.as_ref(),
            )
            .await;
            let usage = invocation.session.token_usage_info().await;
            let (turn_active, turn_kind) = {
                let active = invocation.session.active_turn.lock().await;
                active
                    .as_ref()
                    .and_then(|active_turn| active_turn.task.as_ref())
                    .filter(|task| task.turn_context.sub_id == step.turn.sub_id)
                    .map_or((false, None), |task| {
                        let kind = match task.kind {
                            TaskKind::Regular => "regular",
                            TaskKind::Review => "review",
                            TaskKind::Compact => "compact",
                        };
                        (true, Some(kind))
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
                model: step.settings.model_info.slug.clone(),
                model_provider: step.turn.config.model_provider_id.clone(),
                reasoning_effort: step
                    .settings
                    .effective_reasoning_effort()
                    .map(|effort| effort.to_string())
                    .unwrap_or_else(|| "default".to_string()),
                thread_id: invocation.session.thread_id.to_string(),
                turn_id: step.turn.sub_id.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::step_context::StepContext;
    use crate::session::tests::make_session_and_context;
    use crate::session::tests::update_selected_settings_for_test;
    use crate::tools::context::ToolCallSource;
    use crate::turn_diff_tracker::TurnDiffTracker;
    use codex_protocol::openai_models::ReasoningEffort;
    use pretty_assertions::assert_eq;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn context_status_uses_invoking_step_not_initial_turn_model() {
        let (session, turn) = make_session_and_context().await;
        let turn = Arc::new(turn);
        let mut step = StepContext::for_test(Arc::clone(&turn));
        let settings = Arc::make_mut(&mut Arc::get_mut(&mut step).expect("owned step").settings);
        let model = Arc::make_mut(&mut settings.model_info);
        model.slug = "step-model".to_string();
        model.context_window = Some(20_000);
        model.effective_context_window_percent = 100;
        update_selected_settings_for_test(settings, |selected| {
            selected.collaboration_mode.settings.reasoning_effort = Some(ReasoningEffort::High);
        });
        let thread_id = session.thread_id.to_string();
        let turn_id = turn.sub_id.clone();
        let payload = ToolPayload::Function {
            arguments: "{}".to_string(),
        };
        let output = ContextStatusHandler
            .handle(ToolInvocation {
                session: Arc::new(session),
                turn,
                step_context: step,
                cancellation_token: CancellationToken::new(),
                tracker: Arc::new(Mutex::new(TurnDiffTracker::default())),
                call_id: "status-step".to_string(),
                tool_name: ToolName::plain(CONTEXT_STATUS_TOOL_NAME),
                source: ToolCallSource::Direct,
                environments: Default::default(),
                payload: payload.clone(),
            })
            .await
            .expect("context status");
        let result = output.code_mode_result(&payload);
        assert_eq!(
            serde_json::json!({
                "model": result["model"],
                "reasoning_effort": result["reasoning_effort"],
                "context_window_tokens": result["context_window_tokens"],
                "thread_id": result["thread_id"],
                "turn_id": result["turn_id"],
                "turn_active": result["turn_active"],
                "turn_kind": result["turn_kind"],
            }),
            serde_json::json!({
                "model": "step-model", "reasoning_effort": "high",
                "context_window_tokens": 20_000, "thread_id": thread_id,
                "turn_id": turn_id, "turn_active": false, "turn_kind": null,
            })
        );
    }
}
