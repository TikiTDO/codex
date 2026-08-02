use super::*;
use pretty_assertions::assert_eq;

#[test]
fn configured_prompt_is_capped_before_rendering() {
    let prompt = "post-compaction instruction ".repeat(POST_COMPACTION_PROMPT_MAX_TOKENS * 2);
    let expected = truncate_text(
        prompt.trim(),
        TruncationPolicy::Tokens(POST_COMPACTION_PROMPT_MAX_TOKENS),
    );

    let context = PostCompactionContext::new(Some(&prompt));

    assert_eq!(context.configured_prompt, Some(expected));
}
