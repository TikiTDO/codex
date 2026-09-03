pub const SUMMARIZATION_PROMPT: &str = include_str!("../templates/compact/prompt.md");
pub const SUMMARY_PREFIX: &str = include_str!("../templates/compact/summary_prefix.md");
pub const POST_COMPACTION_MARKER: &str = include_str!("../templates/compact/post_compaction.md");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compaction_is_same_conversation_context_reduction() {
        assert!(SUMMARIZATION_PROMPT.contains("this same conversation"));
        assert!(SUMMARY_PREFIX.contains("This conversation's earlier context"));
        assert!(POST_COMPACTION_MARKER.contains("same conversation and the same working hand"));
        assert!(
            POST_COMPACTION_MARKER.contains("does not create, clear, complete, block, or transfer")
        );

        let combined =
            format!("{SUMMARIZATION_PROMPT}\n{SUMMARY_PREFIX}\n{POST_COMPACTION_MARKER}");
        assert!(!combined.contains("another LLM"));
        assert!(!combined.contains("Another language model"));
        assert!(!combined.contains("other language model"));
    }
}
