use super::*;
use codex_prompts::REPOSITORY_QUESTION_ROUTING;
use codex_prompts::ResolvedModelMessages;
use pretty_assertions::assert_eq;
use serde_json::json;

#[test]
fn request_user_input_async_final_catalog_description_preserves_parameters() {
    let parameters = json!({
        "type": "object",
        "properties": {"catalog_choice": {"type": "string"}},
        "required": ["catalog_choice"],
        "additionalProperties": false,
    });
    let description = "Distinct catalog-provided question description.\n";
    let handler = RequestUserInputAsyncHandler {
        description: description.to_string(),
        parameters: Some(parameters.to_string()),
    };
    assert_eq!(
        handler.spec(),
        ToolSpec::Function(ResponsesApiTool {
            name: "request_user_input_async".to_string(),
            description: format!("{description}\n\n{REPOSITORY_QUESTION_ROUTING}"),
            strict: false,
            defer_loading: None,
            parameters: serde_json::from_value(parameters).expect("catalog schema"),
            output_schema: None,
        })
    );
}

#[test]
fn request_user_input_async_final_bundled_description_preserves_questions_schema() {
    let description = ResolvedModelMessages::bundled().request_user_input_async_description();
    let handler = RequestUserInputAsyncHandler {
        description: description.to_string(),
        parameters: None,
    };
    let parameters = json!({
        "type": "object",
        "properties": {
            "questions": {
                "type": "array",
                "minItems": 1,
                "description": "One or more self-contained questions to present together, in display order.",
                "items": {
                    "type": "object",
                    "properties": {
                        "title": {
                            "type": "string",
                            "description": "The complete question shown to the user, including any context needed to answer it.",
                        },
                        "options": {
                            "type": "array",
                            "minItems": 1,
                            "items": {"type": "string"},
                            "description": "Suggested answers, in display order. Put the recommended answer first; the first option is preselected by default. The user can select one option or enter a free-text answer. Do not include an Other option or a free-text placeholder; the UI provides free-text input automatically. Omit options for a free-text-only question.",
                        },
                    },
                    "required": ["title"],
                    "additionalProperties": false,
                },
            },
        },
        "required": ["questions"],
        "additionalProperties": false,
    });
    assert_eq!(
        handler.spec(),
        ToolSpec::Function(ResponsesApiTool {
            name: "request_user_input_async".to_string(),
            description: format!("{description}\n\n{REPOSITORY_QUESTION_ROUTING}"),
            strict: false,
            defer_loading: None,
            parameters: serde_json::from_value(parameters).expect("bundled schema"),
            output_schema: None,
        })
    );
}
