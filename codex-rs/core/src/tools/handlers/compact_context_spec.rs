use codex_tools::JsonSchema;
use codex_tools::ResponsesApiTool;
use codex_tools::ToolSpec;
use std::collections::BTreeMap;

pub(crate) const COMPACT_CONTEXT_TOOL_NAME: &str = "compact_context";

pub fn create_compact_context_tool() -> ToolSpec {
    let mut properties = BTreeMap::new();
    properties.insert(
        "instructions".to_string(),
        JsonSchema::string(Some(
            "Optional guidance for what the compacted context should preserve, summarize, edit, or emphasize."
                .to_string(),
        )),
    );
    properties.insert(
        "discard_images".to_string(),
        JsonSchema::boolean(Some(
            "If true, omit inline image bodies from the compactor input and installed compacted context. The source transcript is unchanged."
                .to_string(),
        )),
    );
    properties.insert(
        "retry_websocket".to_string(),
        JsonSchema::boolean(Some(
            "If true, retry Responses WebSocket transport once after successful compaction; failure returns to sticky HTTP fallback."
                .to_string(),
        )),
    );
    ToolSpec::Function(ResponsesApiTool {
        name: COMPACT_CONTEXT_TOOL_NAME.to_string(),
        description: "Schedule context compaction for the current turn. The compaction runs after this tool result and before the next model step, preserving environment state while replacing conversation history with compacted context. It continues the same conversation and does not create, clear, complete, block, or transfer a task or goal."
            .to_string(),
        strict: false,
        defer_loading: None,
        parameters: JsonSchema::object(properties, /*required*/ None, Some(false.into())),
        output_schema: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn description_preserves_conversation_and_goal_lifecycle() {
        let ToolSpec::Function(tool) = create_compact_context_tool() else {
            panic!("compact_context should be a function tool")
        };
        assert!(tool.description.contains("continues the same conversation"));
        assert!(
            tool.description
                .contains("does not create, clear, complete, block, or transfer a task or goal")
        );
    }
}
