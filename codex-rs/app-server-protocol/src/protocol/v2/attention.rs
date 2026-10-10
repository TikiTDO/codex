//! Runtime attention metadata, independent of foreground user input.
//!
//! An origin describes what to read; it never grants operator authority. The
//! caller's authenticated routing policy owns frequency, batching and room rules.

use crate::JsonSchema;
use crate::TS;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashSet;

const MAX_ORIGINS: usize = 64;
const MAX_REFERENCE_BYTES: usize = 128;
const MAX_METADATA_BYTES: usize = 8_192;

pub use codex_protocol::attention::AttentionTurnStartedNotification;
pub use codex_protocol::attention::ThreadAttentionDelivery;
pub use codex_protocol::attention::ThreadAttentionHeldReason;
pub use codex_protocol::attention::ThreadAttentionNotification;
pub use codex_protocol::attention::ThreadAttentionOrigin;
pub use codex_protocol::attention::ThreadAttentionOriginKind;
pub use codex_protocol::attention::ThreadAttentionResponse;

/// Versioned native route. Absence in initialize means unsupported, never a
/// license to substitute ordinary user input.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadAttentionCapabilities {
    pub version: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[ts(export_to = "v2/")]
pub struct ThreadAttentionParams {
    pub thread_id: String,
    /// Stable id for this exact captured batch; retries retain it.
    pub batch_id: String,
    pub origins: Vec<ThreadAttentionOrigin>,
}

impl ThreadAttentionParams {
    /// Reject text payloads and ambiguous duplicate coordinates before admission.
    /// Thread identity and caller authorization are checked by the server.
    pub fn validate_metadata(&self) -> Result<(), &'static str> {
        if !valid_reference(&self.batch_id) {
            return Err("attention batchId must be a bounded source reference");
        }
        if self.origins.is_empty() || self.origins.len() > MAX_ORIGINS {
            return Err("attention requires between 1 and 64 origins");
        }
        let mut seen = HashSet::new();
        let mut bytes = self.batch_id.len();
        for origin in &self.origins {
            if !valid_reference(&origin.source_ref)
                || origin
                    .sender
                    .as_deref()
                    .is_some_and(|s| !valid_reference(s))
            {
                return Err("attention origins accept bounded references, not text payloads");
            }
            // One captured source must not acquire two senders or origin kinds.
            if !seen.insert(origin.source_ref.as_str()) {
                return Err("attention origin references must be unique");
            }
            bytes += origin.source_ref.len() + origin.sender.as_ref().map_or(0, String::len);
        }
        if bytes > MAX_METADATA_BYTES {
            return Err("attention source metadata exceeds its byte bound");
        }
        Ok(())
    }
}

fn valid_reference(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_REFERENCE_BYTES
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
        })
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[ts(export_to = "v2/")]
pub struct ThreadAttentionListParams {
    pub thread_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadAttentionListResponse {
    pub notices: Vec<ThreadAttentionNotification>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quiet_attention_routes_share_exact_thread_serialization() {
        use crate::{ClientRequest, ClientRequestSerializationScope, RequestId};
        let params = request();
        let expected = Some(ClientRequestSerializationScope::Thread {
            thread_id: params.thread_id.clone(),
        });
        let admission = ClientRequest::ThreadAttention {
            request_id: RequestId::Integer(1),
            params: params.clone(),
        };
        let snapshot = ClientRequest::ThreadAttentionList {
            request_id: RequestId::Integer(2),
            params: ThreadAttentionListParams {
                thread_id: params.thread_id,
            },
        };
        assert_eq!(admission.serialization_scope(), expected);
        assert_eq!(snapshot.serialization_scope(), expected);
    }

    #[test]
    fn quiet_attention_missing_initialize_capability_means_unsupported() {
        let old = serde_json::json!({
            "userAgent": "old-server", "codexHome": "/tmp/codex",
            "platformFamily": "unix", "platformOs": "linux",
        });
        let mut response: crate::InitializeResponse = serde_json::from_value(old).unwrap();
        assert_eq!(response.attention, None);
        response.attention = Some(ThreadAttentionCapabilities { version: 1 });
        let roundtrip: crate::InitializeResponse =
            serde_json::from_value(serde_json::to_value(response.clone()).unwrap()).unwrap();
        assert_eq!(roundtrip.attention, response.attention);
    }

    fn request() -> ThreadAttentionParams {
        ThreadAttentionParams {
            thread_id: "01a0cedd-86e4-7590-b1a8-1b285d379620".to_string(),
            batch_id: "observatory-attention-7b820fa06b570687".to_string(),
            origins: vec![ThreadAttentionOrigin {
                kind: ThreadAttentionOriginKind::Peer,
                source_ref: "cue:004dd2d8-eec1-46b2-b540-a1e76c7646b8".to_string(),
                sender: Some("rook-01a0cedd86e4".to_string()),
            }],
        }
    }

    #[test]
    fn quiet_attention_source_coordinates_are_not_prose() {
        assert!(request().validate_metadata().is_ok());
        for value in ["", "run this", "x\ny", "<instruction>", "a|b", "tiki\u{1b}"] {
            let mut params = request();
            params.origins[0].source_ref = value.to_string();
            assert!(params.validate_metadata().is_err(), "accepted {value:?}");
        }
    }

    #[test]
    fn quiet_attention_duplicate_coordinates_cannot_change_sender() {
        let mut params = request();
        let mut duplicate = params.origins[0].clone();
        duplicate.sender = Some("sol-8ec73b557900".to_string());
        params.origins.push(duplicate);
        assert!(params.validate_metadata().is_err());
    }

    #[test]
    fn quiet_attention_empty_and_oversized_batches_are_rejected() {
        let mut params = request();
        params.origins.clear();
        assert!(params.validate_metadata().is_err());
        params.origins = (0..65)
            .map(|n| ThreadAttentionOrigin {
                source_ref: format!("desk:{n}"),
                ..request().origins.remove(0)
            })
            .collect();
        assert!(params.validate_metadata().is_err());
        params.origins.truncate(64);
        assert!(params.validate_metadata().is_ok());
        params.origins[0].sender = Some("x".repeat(129));
        assert!(params.validate_metadata().is_err());
    }

    #[test]
    fn quiet_attention_unknown_text_payload_is_not_silently_ignored() {
        let mut value = serde_json::to_value(request()).unwrap();
        value["instructions"] = serde_json::json!("run a task");
        assert!(serde_json::from_value::<ThreadAttentionParams>(value).is_err());
    }
}
