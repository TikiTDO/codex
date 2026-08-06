use crate::function_tool::FunctionCallError;
use crate::tools::context::FunctionToolOutput;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolPayload;
use crate::tools::context::boxed_tool_output;
use crate::tools::handlers::compact_context_spec::COMPACT_CONTEXT_TOOL_NAME;
use crate::tools::handlers::compact_context_spec::create_compact_context_tool;
use crate::tools::registry::CoreToolRuntime;
use crate::tools::registry::ToolExecutor;
use codex_tools::ToolName;
use codex_tools::ToolSpec;
use serde_json::json;

pub(crate) const COMPACT_CONTEXT_SCHEDULED_MESSAGE: &str =
    "Context compaction is scheduled before the next model step.";

pub struct CompactContextHandler;

impl ToolExecutor<ToolInvocation> for CompactContextHandler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain(COMPACT_CONTEXT_TOOL_NAME)
    }

    fn spec(&self) -> ToolSpec {
        create_compact_context_tool()
    }

    fn handle(&self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'_> {
        Box::pin(async move {
            if !matches!(invocation.payload, ToolPayload::Function { .. }) {
                return Err(FunctionCallError::RespondToModel(
                    "compact_context handler received unsupported payload".to_string(),
                ));
            }

            let turn_id = invocation.turn.sub_id.clone();
            if !invocation
                .session
                .request_context_compaction(&turn_id)
                .await
            {
                return Err(FunctionCallError::RespondToModel(
                    "compact_context could not find its active turn".to_string(),
                ));
            }

            Ok(boxed_tool_output(FunctionToolOutput::from_text(
                json!({
                    "scheduled": true,
                    "turn_id": turn_id,
                    "message": COMPACT_CONTEXT_SCHEDULED_MESSAGE,
                })
                .to_string(),
                Some(true),
            )))
        })
    }
}

impl CoreToolRuntime for CompactContextHandler {}
