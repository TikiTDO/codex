use std::io;

use chrono::DateTime;
use chrono::FixedOffset;
use png::text_metadata::EncodableTextChunk;
use png::text_metadata::ITXtChunk;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

const MAX_TITLE_CHARS: usize = 120;
const MAX_METADATA_FIELD_CHARS: usize = 8_000;
const MAX_METADATA_LIST_ITEMS: usize = 16;
const MAX_METADATA_JSON_BYTES: usize = 32 * 1024;
const PNG_SIGNATURE: &[u8; 8] = b"\x89PNG\r\n\x1a\n";
const PNG_IEND_CHUNK: &[u8; 12] = b"\0\0\0\0IEND\xaeB`\x82";

/// Optional human-readable provenance deliberately selected for the saved image.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ImagegenMetadata {
    /// A short creator-side reading or reflection.
    #[schemars(length(max = 8000))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) thoughts: Option<String>,
    /// Companion text intended to travel with the image.
    #[schemars(length(max = 8000))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) text: Option<String>,
    /// Commissioner statements intentionally selected for preservation.
    #[schemars(length(max = 16))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) commissioner_notes: Option<Vec<String>>,
    /// Comments intentionally pinned to this artifact.
    #[schemars(length(max = 16))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) pinned_comments: Option<Vec<String>>,
}

/// Complete metadata written into one generated PNG.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct ImageArtifactMetadata {
    pub(crate) title: String,
    pub(crate) created_at: DateTime<FixedOffset>,
    pub(crate) model: String,
    #[serde(flatten)]
    pub(crate) creative: ImagegenMetadata,
}

/// Resolves a useful artifact title while preserving compatibility with older callers.
pub(crate) fn resolve_title(title: Option<&str>) -> Result<String, String> {
    if let Some(title) = title {
        let title = title.trim();
        if title.is_empty() {
            return Err("`title` must not be empty when provided".to_string());
        }
        if title.chars().count() > MAX_TITLE_CHARS {
            return Err(format!(
                "`title` must contain at most {MAX_TITLE_CHARS} characters"
            ));
        }
        return Ok(title.to_string());
    }

    Ok("generated image".to_string())
}

/// Rejects unexpectedly large metadata before spending an image-generation request.
pub(crate) fn validate_metadata(metadata: &ImagegenMetadata) -> Result<(), String> {
    for (field, value) in [
        ("thoughts", metadata.thoughts.as_deref()),
        ("text", metadata.text.as_deref()),
    ] {
        if value.is_some_and(|value| value.chars().count() > MAX_METADATA_FIELD_CHARS) {
            return Err(format!(
                "`metadata.{field}` must contain at most {MAX_METADATA_FIELD_CHARS} characters"
            ));
        }
    }
    for (field, values) in [
        ("commissioner_notes", metadata.commissioner_notes.as_deref()),
        ("pinned_comments", metadata.pinned_comments.as_deref()),
    ] {
        let Some(values) = values else {
            continue;
        };
        if values.len() > MAX_METADATA_LIST_ITEMS {
            return Err(format!(
                "`metadata.{field}` must contain at most {MAX_METADATA_LIST_ITEMS} entries"
            ));
        }
        if values
            .iter()
            .any(|value| value.chars().count() > MAX_METADATA_FIELD_CHARS)
        {
            return Err(format!(
                "each `metadata.{field}` entry must contain at most \
                 {MAX_METADATA_FIELD_CHARS} characters"
            ));
        }
    }

    let bytes = serde_json::to_vec(metadata)
        .map_err(|error| format!("unable to serialize image metadata: {error}"))?;
    if bytes.len() > MAX_METADATA_JSON_BYTES {
        return Err(format!(
            "`metadata` must serialize to at most {MAX_METADATA_JSON_BYTES} bytes"
        ));
    }
    Ok(())
}

/// Adds UTF-8 PNG text and XMP metadata without re-encoding image pixels.
pub(crate) fn embed_png_metadata(
    mut png_bytes: Vec<u8>,
    metadata: &ImageArtifactMetadata,
) -> io::Result<Vec<u8>> {
    if !png_bytes.starts_with(PNG_SIGNATURE) || !png_bytes.ends_with(PNG_IEND_CHUNK) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "image generation result is not a complete PNG",
        ));
    }

    let metadata_json = serde_json::to_string(metadata)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let mut encoded_chunks = Vec::new();
    let mut chunks = vec![
        ITXtChunk::new("Title", metadata.title.clone()),
        ITXtChunk::new("Creation Time", metadata.created_at.to_rfc3339()),
        ITXtChunk::new("Software", "Codex image_gen"),
        ITXtChunk::new("Model", metadata.model.clone()),
        ITXtChunk::new("Codex Metadata", metadata_json.clone()),
        ITXtChunk::new("XML:com.adobe.xmp", xmp_packet(metadata, &metadata_json)),
    ];
    if let Some(value) = metadata.creative.thoughts.as_ref() {
        chunks.push(ITXtChunk::new("Thoughts", value.clone()));
    }
    if let Some(value) = metadata.creative.text.as_ref() {
        chunks.push(ITXtChunk::new("Description", value.clone()));
    }
    if let Some(values) = metadata.creative.commissioner_notes.as_ref() {
        chunks.push(ITXtChunk::new(
            "Commissioner Notes",
            serde_json::to_string(values)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?,
        ));
    }
    if let Some(values) = metadata.creative.pinned_comments.as_ref() {
        chunks.push(ITXtChunk::new(
            "Pinned Comments",
            serde_json::to_string(values)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?,
        ));
    }
    for chunk in chunks {
        chunk
            .encode(&mut encoded_chunks)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    }

    let iend_offset = png_bytes.len() - PNG_IEND_CHUNK.len();
    png_bytes.splice(iend_offset..iend_offset, encoded_chunks);
    Ok(png_bytes)
}

fn xmp_packet(metadata: &ImageArtifactMetadata, metadata_json: &str) -> String {
    format!(
        r#"<?xpacket begin="﻿" id="W5M0MpCehiHzreSzNTczkc9d"?>
<x:xmpmeta xmlns:x="adobe:ns:meta/">
 <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
  <rdf:Description rdf:about=""
   xmlns:dc="http://purl.org/dc/elements/1.1/"
   xmlns:xmp="http://ns.adobe.com/xap/1.0/"
   xmlns:codex="https://openai.com/ns/codex/1.0/"
   xmp:CreatorTool="Codex image_gen"
   xmp:CreateDate="{created_at}"
   codex:Model="{model}">
   <dc:title><rdf:Alt><rdf:li xml:lang="x-default">{title}</rdf:li></rdf:Alt></dc:title>
   <codex:MetadataJson>{metadata_json}</codex:MetadataJson>
  </rdf:Description>
 </rdf:RDF>
</x:xmpmeta>
<?xpacket end="w"?>"#,
        created_at = xml_escape(&metadata.created_at.to_rfc3339()),
        model = xml_escape(&metadata.model),
        title = xml_escape(&metadata.title),
        metadata_json = xml_escape(metadata_json),
    )
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
