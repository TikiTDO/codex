/// Repository routing is appended at the final tool consumer so catalog-provided
/// descriptions retain their text without bypassing the repository's question route.
pub const REPOSITORY_QUESTION_ROUTING: &str = "Repository question routing: When the active repository provides Desk and directs operator questions there, use its Desk question, choice, review and validation features instead of request_user_input or request_user_input_async. Follow the repository's documented Desk commands and answer/closure workflow. If the required route is unavailable, report the hold; do not silently fall back to the native question tool. Conversation questions are not native execution, permission or security approval mechanisms, which remain on their existing routes. Outside such a repository, use this tool normally.";

pub fn with_repository_question_routing(description: &str) -> String {
    format!("{description}\n\n{REPOSITORY_QUESTION_ROUTING}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repository_question_routing_preserves_the_original_description() {
        for description in [
            "",
            "Catalog-provided description.\n",
            "  Bundled description.  ",
        ] {
            assert_eq!(
                with_repository_question_routing(description),
                format!("{description}\n\n{REPOSITORY_QUESTION_ROUTING}")
            );
        }
    }
}
