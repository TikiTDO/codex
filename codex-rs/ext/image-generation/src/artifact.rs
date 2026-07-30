use std::fmt::Display;

use chrono::DateTime;
use chrono::FixedOffset;
use codex_utils_absolute_path::AbsolutePathBuf;

const MAX_IMAGE_GENERATION_OUTPUT_HINT_BYTES: usize = 1024;
const MAX_TITLE_SLUG_BYTES: usize = 80;

/// Returns the extension-owned artifact path for a generated image.
pub(crate) fn image_generation_artifact_path(
    output_dir: &AbsolutePathBuf,
    created_at: &DateTime<FixedOffset>,
    title: &str,
    call_id: &str,
) -> AbsolutePathBuf {
    output_dir
        .join(created_at.format("%Y-%m-%d").to_string())
        .join(format!(
            "{}-{}-{}.png",
            created_at.format("%Y%m%d-%H%M%S-%3f"),
            title_slug(title),
            identifier_slug(call_id),
        ))
}

fn title_slug(value: &str) -> String {
    let mut slug = String::with_capacity(value.len().min(MAX_TITLE_SLUG_BYTES));
    let mut separator_pending = false;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            if separator_pending && !slug.is_empty() {
                if slug.len() + 1 >= MAX_TITLE_SLUG_BYTES {
                    break;
                }
                slug.push('-');
            }
            separator_pending = false;
            if slug.len() >= MAX_TITLE_SLUG_BYTES {
                break;
            }
            slug.push(ch.to_ascii_lowercase());
        } else {
            separator_pending = true;
        }
    }
    let slug = slug.trim_end_matches('-');
    if slug.is_empty() {
        "generated-image".to_string()
    } else {
        slug.to_string()
    }
}

fn identifier_slug(value: &str) -> String {
    let slug: String = value
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .take(12)
        .collect();
    if slug.is_empty() {
        "call".to_string()
    } else {
        slug
    }
}

/// Returns the model-facing generated-image path hint, or omits it if it is too large.
pub(crate) fn image_generation_output_hint(
    image_output_dir: impl Display,
    image_output_path: impl Display,
) -> Option<String> {
    let hint = format!(
        "Generated images are saved to {image_output_dir} as {image_output_path} by default.\nIf you need to use a generated image at another path, copy it and leave the original in place unless the user explicitly asks you to delete it.\nThe generated image is already displayed to the user. There is no need to render it in the final response as a Markdown image or file link."
    );
    (hint.len() <= MAX_IMAGE_GENERATION_OUTPUT_HINT_BYTES).then_some(hint)
}
