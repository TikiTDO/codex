use std::io::Cursor;

use chrono::DateTime;
use codex_api::ImageBackground;
use codex_api::ImageEditRequest;
use codex_api::ImageGenerationRequest;
use codex_api::ImageQuality;
use codex_api::ImageUrl;
use codex_extension_api::ToolOutput;
use codex_extension_api::ToolPayload;
use codex_extension_api::ToolSpec;
use codex_protocol::ResponseItemId;
use codex_protocol::models::ContentItem;
use codex_protocol::models::DEFAULT_IMAGE_DETAIL;
use codex_protocol::models::FunctionCallOutputBody;
use codex_protocol::models::FunctionCallOutputContentItem;
use codex_protocol::models::FunctionCallOutputPayload;
use codex_protocol::models::ResponseInputItem;
use codex_protocol::models::ResponseItem;
use codex_tools::ResponsesApiNamespaceTool;
use codex_utils_absolute_path::AbsolutePathBuf;
use pretty_assertions::assert_eq;

use super::GeneratedImageOutput;
use super::ImageRequest;
use super::ImagegenArgs;
use super::imagegen_tool_spec;
use super::request_for_call_args;
use crate::IMAGE_GEN_NAMESPACE;
use crate::IMAGEGEN_TOOL_NAME;
use crate::artifact::image_generation_artifact_path;
use crate::artifact::image_generation_output_hint;
use crate::metadata::ImageArtifactMetadata;
use crate::metadata::ImagegenMetadata;
use crate::metadata::embed_png_metadata;
use crate::metadata::resolve_title;
use crate::metadata::validate_metadata;

const RESULT: &str = "cG5n";
const TINY_PNG_BYTES: &[u8] = &[
    137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 1, 0, 0, 0, 1, 8, 6, 0,
    0, 0, 31, 21, 196, 137, 0, 0, 0, 13, 73, 68, 65, 84, 120, 156, 99, 248, 207, 192, 240, 31, 0,
    5, 0, 1, 255, 137, 153, 61, 29, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66, 96, 130,
];

#[test]
fn artifact_path_uses_local_date_timestamp_title_and_call_id() {
    let output_dir = AbsolutePathBuf::current_dir().expect("current directory should be absolute");
    let created_at =
        DateTime::parse_from_rfc3339("2026-07-30T10:45:12.123-04:00").expect("valid timestamp");

    assert_eq!(
        image_generation_artifact_path(
            &output_dir,
            &created_at,
            "The First Window!",
            "call_1234567890-extra",
        ),
        output_dir
            .join("2026-07-30")
            .join("20260730-104512-123-the-first-window-call12345678.png")
    );
}

#[test]
fn saved_png_contains_selected_utf8_and_xmp_metadata() {
    let created_at =
        DateTime::parse_from_rfc3339("2026-07-30T10:45:12.123-04:00").expect("valid timestamp");
    let bytes = embed_png_metadata(
        TINY_PNG_BYTES.to_vec(),
        &ImageArtifactMetadata {
            title: "The First Window".to_string(),
            created_at,
            model: "gpt-image-2".to_string(),
            creative: ImagegenMetadata {
                thoughts: Some("Perception before assignment.".to_string()),
                text: Some("The first gift is a window.".to_string()),
                commissioner_notes: Some(vec!["It is luck.".to_string()]),
                pinned_comments: Some(vec!["Keep the quad.".to_string()]),
            },
        },
    )
    .expect("metadata should embed");

    let mut reader = png::Decoder::new(Cursor::new(bytes))
        .read_info()
        .expect("saved artifact should remain a valid PNG");
    let mut pixels = vec![
        0;
        reader
            .output_buffer_size()
            .expect("decoded image should have a bounded size")
    ];
    reader
        .next_frame(&mut pixels)
        .expect("saved artifact should decode");
    reader
        .finish()
        .expect("saved artifact trailing metadata should decode");
    let chunks = &reader.info().utf8_text;
    let text_for = |keyword: &str| {
        chunks
            .iter()
            .find(|chunk| chunk.keyword == keyword)
            .expect("metadata keyword should be present")
            .get_text()
            .expect("metadata text should decode")
    };

    assert_eq!(text_for("Title"), "The First Window");
    assert_eq!(text_for("Thoughts"), "Perception before assignment.");
    assert_eq!(text_for("Commissioner Notes"), r#"["It is luck."]"#);
    assert!(text_for("XML:com.adobe.xmp").contains("The First Window"));
}

#[test]
fn omitted_title_uses_privacy_safe_fallback() {
    assert_eq!(
        resolve_title(None).expect("omitted title should preserve older callers"),
        "generated image"
    );
    assert_eq!(
        resolve_title(Some("   ")).expect_err("blank explicit title should be rejected"),
        "`title` must not be empty when provided"
    );
}

#[test]
fn creative_metadata_is_bounded_before_generation() {
    let metadata = ImagegenMetadata {
        thoughts: Some("x".repeat(8_001)),
        ..ImagegenMetadata::default()
    };

    assert_eq!(
        validate_metadata(&metadata).expect_err("oversized metadata should be rejected"),
        "`metadata.thoughts` must contain at most 8000 characters"
    );
}

#[test]
fn uses_reserved_image_gen_namespace() {
    let ToolSpec::Namespace(spec) = imagegen_tool_spec() else {
        panic!("imagegen should advertise a namespace tool");
    };
    assert_eq!(spec.name, IMAGE_GEN_NAMESPACE);
    let ResponsesApiNamespaceTool::Function(function) = &spec.tools[0];
    assert_eq!(function.name, IMAGEGEN_TOOL_NAME);
}

#[tokio::test]
async fn omitted_references_generate_with_fixed_defaults() {
    assert_eq!(
        request_for_call_args(
            &ImagegenArgs {
                prompt: "paint a moonlit lake".to_string(),
                title: None,
                metadata: None,
                referenced_image_paths: None,
                num_last_images_to_include: None,
            },
            &[],
            &[],
        )
        .await
        .expect("generation request should build"),
        ImageRequest::Generate(ImageGenerationRequest {
            prompt: "paint a moonlit lake".to_string(),
            background: Some(ImageBackground::Auto),
            model: "gpt-image-2".to_string(),
            n: None,
            quality: Some(ImageQuality::Auto),
            size: Some("auto".to_string()),
        })
    );
}

#[tokio::test]
async fn recent_image_fallback_selects_newest_images_in_chronological_order() {
    let history = vec![
        ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![
                input_image("user-1"),
                input_image("user-2"),
                ContentItem::InputText {
                    text: "edit these".to_string(),
                },
            ],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        },
        ResponseItem::FunctionCall {
            id: None,
            name: "mcp_image".to_string(),
            namespace: None,
            arguments: "{}".to_string(),
            call_id: "mcp-call".to_string(),
            encrypted_function_args: None,
            internal_chat_message_metadata_passthrough: None,
        },
        ResponseItem::FunctionCallOutput {
            id: None,
            call_id: "mcp-call".to_string(),
            output: image_output("mcp"),
            internal_chat_message_metadata_passthrough: None,
        },
        ResponseItem::CustomToolCall {
            id: None,
            status: Some("completed".to_string()),
            call_id: "code-mode-call".to_string(),
            name: "exec".to_string(),
            namespace: None,
            input: String::new(),
            internal_chat_message_metadata_passthrough: None,
        },
        ResponseItem::CustomToolCallOutput {
            id: None,
            call_id: "code-mode-call".to_string(),
            name: Some("exec".to_string()),
            output: image_output("code-mode"),
            internal_chat_message_metadata_passthrough: None,
        },
        ResponseItem::ImageGenerationCall {
            id: Some(ResponseItemId::with_suffix("ig", "generated-call")),
            status: "completed".to_string(),
            revised_prompt: None,
            result: "generated".to_string(),
            internal_chat_message_metadata_passthrough: None,
        },
        ResponseItem::FunctionCallOutput {
            id: None,
            call_id: "orphan-call".to_string(),
            output: image_output("orphan"),
            internal_chat_message_metadata_passthrough: None,
        },
    ];

    assert_eq!(
        request_for_call_args(
            &ImagegenArgs {
                prompt: "change the lighting".to_string(),
                title: None,
                metadata: None,
                referenced_image_paths: None,
                num_last_images_to_include: Some(4),
            },
            &history,
            &[],
        )
        .await
        .expect("history-backed edit request should build"),
        ImageRequest::Edit(expected_edit_request(
            "change the lighting",
            &["user-2", "mcp", "code-mode", "generated"],
        ))
    );
}

#[tokio::test]
async fn conflicting_image_selectors_return_tool_error() {
    let error = request_for_call_args(
        &ImagegenArgs {
            prompt: "change the lighting".to_string(),
            title: None,
            metadata: None,
            referenced_image_paths: Some(vec![
                "/tmp/image.png"
                    .try_into()
                    .expect("test path should be absolute"),
            ]),
            num_last_images_to_include: Some(1),
        },
        &[],
        &[],
    )
    .await
    .expect_err("conflicting selectors should fail");

    assert_eq!(
        error.to_string(),
        "provide only one of `referenced_image_paths` or `num_last_images_to_include`"
    );
}

#[tokio::test]
async fn too_many_referenced_image_paths_return_tool_error() {
    let error = request_for_call_args(
        &ImagegenArgs {
            prompt: "change the lighting".to_string(),
            title: None,
            metadata: None,
            referenced_image_paths: Some(
                (0..6)
                    .map(|index| {
                        format!("/tmp/image-{index}.png")
                            .try_into()
                            .expect("test path should be absolute")
                    })
                    .collect(),
            ),
            num_last_images_to_include: None,
        },
        &[],
        &[],
    )
    .await
    .expect_err("too many paths should fail before reading files");

    assert_eq!(
        error.to_string(),
        "`referenced_image_paths` must contain at most 5 paths"
    );
}

#[tokio::test]
async fn recent_image_fallback_requires_requested_count() {
    let error = request_for_call_args(
        &ImagegenArgs {
            prompt: "change the lighting".to_string(),
            title: None,
            metadata: None,
            referenced_image_paths: None,
            num_last_images_to_include: Some(2),
        },
        &[ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![input_image("only-image")],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        }],
        &[],
    )
    .await
    .expect_err("history-backed edit should require the requested image count");

    assert_eq!(
        error.to_string(),
        "requested the last 2 conversation images, but only 1 were available"
    );
}

#[test]
fn generated_output_returns_image_input_and_output_hint() {
    let output_hint =
        image_generation_output_hint("/tmp", "/tmp/call-1.png").expect("hint should fit");
    let output = GeneratedImageOutput {
        result: RESULT.to_string(),
        output_hint: Some(output_hint.clone()),
    };

    let ResponseInputItem::FunctionCallOutput {
        output: response_output,
        ..
    } = output.to_response_item("call-1", &function_payload())
    else {
        panic!("imagegen should return function tool output");
    };
    let FunctionCallOutputBody::ContentItems(content_items) = response_output.body else {
        panic!("imagegen output should contain generated image bytes");
    };
    assert_eq!(
        content_items,
        vec![
            FunctionCallOutputContentItem::InputImage {
                image_url: format!("data:image/png;base64,{RESULT}"),
                detail: Some(DEFAULT_IMAGE_DETAIL),
            },
            FunctionCallOutputContentItem::InputText { text: output_hint },
        ]
    );
}

#[test]
fn generated_output_returns_generated_image_helper_input_in_code_mode() {
    let output = GeneratedImageOutput {
        result: RESULT.to_string(),
        output_hint: Some("generated image save hint".to_string()),
    };

    assert_eq!(
        output.code_mode_result(&function_payload()),
        serde_json::json!({
            "image_url": format!("data:image/png;base64,{RESULT}"),
            "output_hint": "generated image save hint",
        })
    );
}

#[test]
fn generated_output_omits_oversized_output_hint() {
    let long_path = "x".repeat(1024);
    let output = GeneratedImageOutput {
        result: RESULT.to_string(),
        output_hint: image_generation_output_hint("/tmp", long_path),
    };

    let ResponseInputItem::FunctionCallOutput {
        output: response_output,
        ..
    } = output.to_response_item("call-1", &function_payload())
    else {
        panic!("imagegen should return function tool output");
    };
    let FunctionCallOutputBody::ContentItems(content_items) = response_output.body else {
        panic!("imagegen output should contain generated image bytes");
    };
    assert_eq!(
        content_items,
        vec![FunctionCallOutputContentItem::InputImage {
            image_url: format!("data:image/png;base64,{RESULT}"),
            detail: Some(DEFAULT_IMAGE_DETAIL),
        }]
    );
}

fn input_image(image: &str) -> ContentItem {
    ContentItem::InputImage {
        image_url: format!("data:image/png;base64,{image}"),
        detail: None,
    }
}

fn image_output(image: &str) -> FunctionCallOutputPayload {
    FunctionCallOutputPayload::from_content_items(vec![FunctionCallOutputContentItem::InputImage {
        image_url: format!("data:image/png;base64,{image}"),
        detail: None,
    }])
}

fn expected_edit_request(prompt: &str, images: &[&str]) -> ImageEditRequest {
    ImageEditRequest {
        images: images
            .iter()
            .map(|image| ImageUrl {
                image_url: format!("data:image/png;base64,{image}"),
            })
            .collect(),
        prompt: prompt.to_string(),
        background: Some(ImageBackground::Auto),
        model: "gpt-image-2".to_string(),
        n: None,
        quality: Some(ImageQuality::Auto),
        size: Some("auto".to_string()),
    }
}

fn function_payload() -> ToolPayload {
    ToolPayload::Function {
        arguments: "{}".to_string(),
    }
}
