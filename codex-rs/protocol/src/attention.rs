//! Runtime attention is a source coordinate, not foreground user authorization.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use ts_rs::TS;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub enum ThreadAttentionOriginKind {
    Peer,
    Desk,
    DeskComment,
    Monitor,
    Periodic,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[ts(export_to = "v2/")]
pub struct ThreadAttentionOrigin {
    pub kind: ThreadAttentionOriginKind,
    pub source_ref: String,
    /// A routing label, never authentication or operator authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional = nullable)]
    pub sender: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub enum ThreadAttentionDelivery {
    Started,
    Buffered,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub enum ThreadAttentionHeldReason {
    BlockingRequest,
    Busy,
    PlanMode,
    PendingTriggerTurn,
    ServerDraining,
    /// A durable intent exists but no final receipt survived. Never retry it blindly.
    AdmissionUncertain,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(tag = "status", rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub enum ThreadAttentionResponse {
    /// Context admission only, not model consumption or operator authorization.
    #[serde(rename_all = "camelCase")]
    Accepted {
        delivery: ThreadAttentionDelivery,
        #[ts(optional = nullable)]
        turn_id: Option<String>,
    },
    Held {
        reason: ThreadAttentionHeldReason,
    },
}

/// A thread-scoped notice persists independently of model-visible messages.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadAttentionNotification {
    pub thread_id: String,
    pub batch_id: String,
    pub origins: Vec<ThreadAttentionOrigin>,
    pub admission: ThreadAttentionResponse,
}

/// Core emits this before spawning an idle attention task. It is provenance,
/// not an admission receipt, and cannot be supplied through a turn-trigger label.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct AttentionTurnStartedNotification {
    pub thread_id: String,
    pub turn_id: String,
}

/// Internal receipt operation errors; unavailable history is never an empty success.
#[derive(Debug, thiserror::Error)]
pub enum AttentionError {
    #[error("attention receipt binding rejected: {0}")]
    InvalidBinding(String),
    #[error("attention receipts unavailable: {0}")]
    Unavailable(String),
    #[error("attention session stopped before admission")]
    Stopped,
}

/// The single metadata contract shared by transport, Core and strict recovery.
pub fn validate_attention_metadata(
    batch_id: &str,
    origins: &[ThreadAttentionOrigin],
) -> Result<(), &'static str> {
    fn valid_reference(value: &str) -> bool {
        !value.is_empty()
            && value.len() <= 128
            && value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
            })
    }
    if !valid_reference(batch_id) {
        return Err("attention batchId must be a bounded source reference");
    }
    if origins.is_empty() || origins.len() > 64 {
        return Err("attention requires between 1 and 64 origins");
    }
    let mut seen = std::collections::HashSet::new();
    let mut bytes = batch_id.len();
    for origin in origins {
        if !valid_reference(&origin.source_ref)
            || origin
                .sender
                .as_deref()
                .is_some_and(|value| !valid_reference(value))
        {
            return Err("attention origins accept bounded references, not text payloads");
        }
        if !seen.insert(origin.source_ref.as_str()) {
            return Err("attention origin references must be unique");
        }
        bytes += origin.source_ref.len() + origin.sender.as_ref().map_or(0, String::len);
    }
    if bytes > 8192 {
        return Err("attention source metadata exceeds its byte bound");
    }
    Ok(())
}
