use super::ContextualUserFragment;
use codex_prompts::POST_COMPACTION_MARKER;
use codex_utils_output_truncation::TruncationPolicy;
use codex_utils_output_truncation::truncate_text;

const POST_COMPACTION_PROMPT_MAX_TOKENS: usize = 2_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PostCompactionContext {
    configured_prompt: Option<String>,
}

impl PostCompactionContext {
    pub(crate) fn new(configured_prompt: Option<&str>) -> Self {
        Self {
            configured_prompt: configured_prompt
                .map(str::trim)
                .filter(|prompt| !prompt.is_empty())
                .map(|prompt| {
                    truncate_text(
                        prompt,
                        TruncationPolicy::Tokens(POST_COMPACTION_PROMPT_MAX_TOKENS),
                    )
                }),
        }
    }
}

#[cfg(test)]
#[path = "post_compaction_context_tests.rs"]
mod tests;

impl ContextualUserFragment for PostCompactionContext {
    fn role(&self) -> &'static str {
        "developer"
    }

    fn requires_separate_message(&self) -> bool {
        true
    }

    fn markers(&self) -> (&'static str, &'static str) {
        Self::type_markers()
    }

    fn type_markers() -> (&'static str, &'static str) {
        ("", "")
    }

    fn body(&self) -> String {
        let mut message = POST_COMPACTION_MARKER.trim_end().to_string();
        if let Some(configured_prompt) = &self.configured_prompt {
            message.push_str("\n\n# Configured post-compaction instructions\n\n");
            message.push_str(configured_prompt);
        }
        message
    }
}
