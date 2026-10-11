//! Complete canonical receipt snapshots, independent of model-history tolerance.
use codex_protocol::ThreadId;
use codex_protocol::attention::ThreadAttentionNotification;

#[derive(Debug)]
pub struct CanonicalAttentionReceipts {
    pub thread_id: ThreadId,
    pub notices: Vec<ThreadAttentionNotification>,
    pub coverage: Vec<AttentionReceiptCoverage>,
}

#[derive(Debug)]
pub struct AttentionReceiptCoverage {
    pub rollout_id: ThreadId,
    pub owner_thread_id: ThreadId,
    pub start_ordinal: u64,
    pub end_ordinal_exclusive: u64,
    pub end_byte_offset: u64,
}
