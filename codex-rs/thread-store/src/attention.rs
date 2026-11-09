//! Complete canonical receipt snapshots, independent of model-history tolerance.
use codex_protocol::ThreadId;
use codex_protocol::attention::{
    AttentionError, ThreadAttentionHeldReason, ThreadAttentionNotification, ThreadAttentionResponse,
};
use std::collections::HashMap;

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

pub fn attention_receipt_is_quarantined(notice: &ThreadAttentionNotification) -> bool {
    matches!(
        notice.admission,
        ThreadAttentionResponse::Accepted { .. }
            | ThreadAttentionResponse::Held {
                reason: ThreadAttentionHeldReason::AdmissionUncertain
            }
    )
}

pub fn reduce_attention_receipts(
    thread_id: ThreadId,
    notices: Vec<ThreadAttentionNotification>,
) -> Result<HashMap<String, ThreadAttentionNotification>, AttentionError> {
    let mut result: HashMap<String, ThreadAttentionNotification> = HashMap::new();
    for notice in notices {
        // Inherited model history never inherits another logical thread's admission.
        if notice.thread_id != thread_id.to_string() {
            continue;
        }
        codex_protocol::attention::validate_attention_metadata(&notice.batch_id, &notice.origins)
            .map_err(|err| AttentionError::Unavailable(err.into()))?;
        if let Some(previous) = result.get(&notice.batch_id) {
            if previous.origins != notice.origins
                || (matches!(previous.admission, ThreadAttentionResponse::Accepted { .. })
                    && previous != &notice)
            {
                return Err(AttentionError::Unavailable(
                    "contradictory canonical receipt binding".into(),
                ));
            }
            // A preserved copy does not introduce another transition. Validate
            // its binding first, then skip before cross-batch quarantine.
            if previous == &notice {
                continue;
            }
        }
        if result.values().any(|previous| {
            previous.batch_id != notice.batch_id
                && attention_receipt_is_quarantined(previous)
                && previous.origins.iter().any(|origin| {
                    notice
                        .origins
                        .iter()
                        .any(|incoming| incoming.source_ref == origin.source_ref)
                })
        }) {
            return Err(AttentionError::Unavailable(
                "contradictory canonical source quarantine".into(),
            ));
        }
        result.insert(notice.batch_id.clone(), notice);
    }
    Ok(result)
}

/// A replay sequence, rather than the display ordering of a receipt list.
pub fn attention_receipts_for_preservation(
    notices: HashMap<String, ThreadAttentionNotification>,
) -> Vec<ThreadAttentionNotification> {
    let mut notices: Vec<_> = notices.into_values().collect();
    notices.sort_by(|a, b| {
        attention_receipt_is_quarantined(a)
            .cmp(&attention_receipt_is_quarantined(b))
            .then_with(|| a.batch_id.cmp(&b.batch_id))
    });
    notices
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_protocol::attention::{
        ThreadAttentionDelivery, ThreadAttentionOrigin, ThreadAttentionOriginKind,
    };

    fn notice(id: ThreadId, batch: &str, uncertain: bool) -> ThreadAttentionNotification {
        ThreadAttentionNotification {
            thread_id: id.to_string(),
            batch_id: batch.into(),
            origins: vec![ThreadAttentionOrigin {
                kind: ThreadAttentionOriginKind::Peer,
                source_ref: "shared:x".into(),
                sender: None,
            }],
            admission: ThreadAttentionResponse::Held {
                reason: if uncertain {
                    ThreadAttentionHeldReason::AdmissionUncertain
                } else {
                    ThreadAttentionHeldReason::BlockingRequest
                },
            },
        }
    }

    #[test]
    fn canonical_attention_reduced_replay_orders_held_before_quarantine_and_is_idempotent() {
        let id = ThreadId::new();
        for uncertain in [false, true] {
            let held = notice(id, "z-held", false);
            let mut admitted = notice(id, "a-admitted", true);
            if !uncertain {
                admitted.admission = ThreadAttentionResponse::Accepted {
                    delivery: ThreadAttentionDelivery::Started,
                    turn_id: Some("turn".into()),
                };
            }
            let chronological = vec![held.clone(), admitted.clone()];
            let reduced = reduce_attention_receipts(id, chronological.clone()).unwrap();
            for reverse in [false, true] {
                let mut entries: Vec<_> = reduced.clone().into_iter().collect();
                if reverse {
                    entries.reverse();
                }
                let replay = attention_receipts_for_preservation(entries.into_iter().collect());
                assert_eq!(replay, chronological);
                let mut prefix_and_copy = chronological.clone();
                prefix_and_copy.extend(replay.clone());
                assert_eq!(
                    reduce_attention_receipts(id, prefix_and_copy).unwrap(),
                    reduced
                );
                assert_eq!(
                    reduce_attention_receipts(id, replay.clone()).unwrap(),
                    reduced
                );
                let mut repeated = replay.clone();
                repeated.extend(replay);
                assert_eq!(reduce_attention_receipts(id, repeated).unwrap(), reduced);
            }
        }
    }

    #[test]
    fn canonical_attention_reduced_replay_never_weakens_binding_or_quarantine() {
        let id = ThreadId::new();
        let first = notice(id, "first", true);
        assert!(
            reduce_attention_receipts(id, vec![first.clone(), notice(id, "second", true)]).is_err()
        );
        let mut accepted = first.clone();
        accepted.admission = ThreadAttentionResponse::Accepted {
            delivery: ThreadAttentionDelivery::Started,
            turn_id: Some("turn".into()),
        };
        assert!(reduce_attention_receipts(id, vec![accepted.clone(), first.clone()]).is_err());
        let mut changed = accepted.clone();
        changed.origins[0].source_ref = "changed".into();
        assert!(reduce_attention_receipts(id, vec![accepted.clone(), changed]).is_err());
        let mut outcome = accepted.clone();
        outcome.admission = ThreadAttentionResponse::Accepted {
            delivery: ThreadAttentionDelivery::Started,
            turn_id: Some("another".into()),
        };
        assert!(reduce_attention_receipts(id, vec![accepted.clone(), outcome]).is_err());
        assert_eq!(
            reduce_attention_receipts(id, vec![accepted.clone(), accepted])
                .unwrap()
                .len(),
            1
        );
        assert!(
            reduce_attention_receipts(ThreadId::new(), vec![first])
                .unwrap()
                .is_empty()
        );
    }
}
