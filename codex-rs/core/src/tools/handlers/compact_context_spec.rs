use codex_tools::JsonSchema;
use codex_tools::ResponsesApiTool;
use codex_tools::ToolSpec;
use std::collections::BTreeMap;

pub(crate) const COMPACT_CONTEXT_TOOL_NAME: &str = "compact_context";

pub fn create_compact_context_tool() -> ToolSpec {
    ToolSpec::Function(ResponsesApiTool {
        name: COMPACT_CONTEXT_TOOL_NAME.to_string(),
        description: "Schedule context compaction for the current turn. The compaction runs after this tool result and before the next model step, preserving environment state while replacing conversation history with compacted context. It continues the same conversation and does not create, clear, complete, block, or transfer a task or goal."
            .to_string(),
        strict: false,
        defer_loading: None,
        parameters: JsonSchema::object(BTreeMap::new(), /*required*/ None, Some(false.into())),
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
