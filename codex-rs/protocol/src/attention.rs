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
