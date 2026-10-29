use codex_tools::JsonSchema;
use codex_tools::ResponsesApiTool;
use codex_tools::ToolSpec;
use std::collections::BTreeMap;

pub(crate) const CONTEXT_STATUS_TOOL_NAME: &str = "context_status";

pub fn create_context_status_tool() -> ToolSpec {
    ToolSpec::Function(ResponsesApiTool {
        name: CONTEXT_STATUS_TOOL_NAME.to_string(),
        description: "Report the current model, active-turn state, context-window usage, and cumulative token usage for this Codex thread. Use this before deciding whether to compact context."
            .to_string(),
        strict: false,
        defer_loading: None,
        parameters: JsonSchema::object(BTreeMap::new(), /*required*/ None, Some(false.into())),
        output_schema: None,
    })
}
