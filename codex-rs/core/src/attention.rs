//! One exact-session receipt owner. Admission uses the existing Core dispatch loop.
use crate::WithTurnExtensionData;
use crate::session::session::Session;
use crate::session::turn_input;
use codex_protocol::ThreadId;
use codex_protocol::attention::AttentionError;
use codex_protocol::attention::ThreadAttentionDelivery;
use codex_protocol::attention::ThreadAttentionHeldReason;
use codex_protocol::attention::ThreadAttentionNotification;
use codex_protocol::attention::ThreadAttentionResponse;
use codex_protocol::models::FunctionCallOutputPayload;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::turn_input::NotSubmittedReason;
use codex_protocol::turn_input::TurnInput;
use codex_protocol::turn_input::TurnInputMode;
use codex_protocol::turn_input::TurnInputRequest;
use codex_protocol::turn_input::TurnInputSubmission;
use codex_rollout::RolloutItem;
use codex_thread_store::LiveThread;
use codex_thread_store::attention_receipt_is_quarantined as quarantined;
use codex_thread_store::reduce_attention_receipts as reduce;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::MutexGuard;

/// The protocol already supports standalone host tool notifications: a function
/// output with no call_id. This is neither a fabricated tool call/reply pair nor
/// an additional reply to an already completed model tool call.
///
/// Construct from the validated receipt coordinates, never source prose. Keeping
/// the payload data-only avoids promoting a peer notice into user/developer input.
pub(crate) fn notification_item(notice: &ThreadAttentionNotification) -> ResponseItem {
    ResponseItem::FunctionCallOutput {
        id: None,
        call_id: None,
        name: Some("attention".to_string()),
        namespace: Some("observatory".to_string()),
        output: FunctionCallOutputPayload::from_text(
            serde_json::json!({
                "type": "attention",
                "thread_id": notice.thread_id,
                "batch_id": notice.batch_id,
                "origins": notice.origins,
            })
            .to_string(),
        ),
        internal_chat_message_metadata_passthrough: None,
    }
}

#[derive(Default)]
pub(crate) struct AttentionLedger {
    state: Mutex<LedgerState>,
}

#[derive(Default)]
enum Readiness {
    #[default]
    Initializing,
    Ready,
    Unavailable(String),
}

#[derive(Default)]
struct LedgerState {
    readiness: Readiness,
    closing: bool,
    notices: HashMap<String, ThreadAttentionNotification>,
}

impl LedgerState {
    async fn ensure_ready(
        &mut self,
        thread_id: ThreadId,
        live: Option<&LiveThread>,
    ) -> Result<(), AttentionError> {
        if self.closing {
            return Err(AttentionError::Stopped);
        }
        match &self.readiness {
            Readiness::Ready => return Ok(()),
            Readiness::Unavailable(reason) => {
                return Err(AttentionError::Unavailable(reason.clone()));
            }
            Readiness::Initializing => {}
        }
        let recovered = async {
            let live = live.ok_or_else(|| {
                AttentionError::Unavailable("no acquired persistence lifecycle".into())
            })?;
            let snapshot = live
                .load_canonical_attention_receipts()
                .await
                .map_err(|err| AttentionError::Unavailable(err.to_string()))?;
            if snapshot.thread_id != thread_id || snapshot.coverage.is_empty() {
                return Err(AttentionError::Unavailable(
                    "wrong identity or incomplete canonical coverage".into(),
                ));
            }
            reduce(thread_id, snapshot.notices)
        }
        .await;
        match recovered {
            Ok(notices) => {
                self.notices = notices;
                self.readiness = Readiness::Ready;
                Ok(())
            }
            Err(err) => {
                self.readiness = Readiness::Unavailable(err.to_string());
                Err(err)
            }
        }
    }

    fn validate(
        &self,
        notice: &ThreadAttentionNotification,
    ) -> Result<Option<ThreadAttentionResponse>, AttentionError> {
        if let Some(previous) = self.notices.get(&notice.batch_id) {
            if previous.origins != notice.origins {
                return Err(AttentionError::InvalidBinding(
                    "batchId cannot change its origins".into(),
                ));
            }
            if quarantined(previous) {
                return Ok(Some(previous.admission.clone()));
            }
        }
        if self.notices.values().any(|previous| {
            previous.batch_id != notice.batch_id
                && quarantined(previous)
                && previous.origins.iter().any(|origin| {
                    notice
                        .origins
                        .iter()
                        .any(|incoming| incoming.source_ref == origin.source_ref)
                })
        }) {
            return Err(AttentionError::InvalidBinding(
                "source already admitted or uncertain; retry its original batchId".into(),
            ));
        }
        Ok(None)
    }
}

impl AttentionLedger {
    pub(crate) async fn snapshot(
        &self,
        thread_id: ThreadId,
        live: Option<&LiveThread>,
    ) -> Result<Vec<ThreadAttentionNotification>, AttentionError> {
        let mut state = self.state.lock().await;
        state.ensure_ready(thread_id, live).await?;
        let mut notices = state.notices.values().cloned().collect::<Vec<_>>();
        notices.sort_by(|a, b| a.batch_id.cmp(&b.batch_id));
        Ok(notices)
    }

    pub(crate) async fn close(&self) {
        self.state.lock().await.closing = true;
    }
}

// Dropping an interrupted or panicking transaction cannot publish Ready after
// possible admission. The intent remains quarantined until strict restart recovery.
struct Transaction<'a> {
    state: MutexGuard<'a, LedgerState>,
    settled: bool,
}
impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.settled {
            self.state.readiness =
                Readiness::Unavailable("attention transaction did not settle".into());
        }
    }
}

async fn persist(
    live: &LiveThread,
    notice: &ThreadAttentionNotification,
) -> Result<(), AttentionError> {
    live.append_items(&[RolloutItem::EventMsg(EventMsg::ThreadAttention(
        notice.clone(),
    ))])
    .await
    .map_err(|err| AttentionError::Unavailable(err.to_string()))?;
    live.flush()
        .await
        .map_err(|err| AttentionError::Unavailable(err.to_string()))
}

pub(crate) async fn handle(
    session: &Arc<Session>,
    mut notice: ThreadAttentionNotification,
    request: TurnInputRequest,
    blocking_request: bool,
    submission_id: String,
) -> Result<ThreadAttentionResponse, AttentionError> {
    let live = session.live_thread();
    let mut state = session.services.attention_ledger.state.lock().await;
    state.ensure_ready(session.thread_id, live).await?;
    if notice.thread_id != session.thread_id.to_string()
        || notice.batch_id.is_empty()
        || notice.origins.is_empty()
        || !request.background_attention
        || request.input != TurnInput::ResponseItem(notification_item(&notice))
    {
        return Err(AttentionError::InvalidBinding(
            "invalid exact-thread attention tool notification".into(),
        ));
    }
    codex_protocol::attention::validate_attention_metadata(&notice.batch_id, &notice.origins)
        .map_err(|err| AttentionError::InvalidBinding(err.into()))?;
    if let Some(previous) = state.validate(&notice)? {
        return Ok(previous);
    }
    let live =
        live.ok_or_else(|| AttentionError::Unavailable("persistence lifecycle missing".into()))?;
    // Existing receipt outcomes return before a fresh execution-capacity check.
    // Only a genuinely new idle admission consults the existing control gate.
    if !blocking_request && session.active_turn.lock().await.is_none() {
        let config = session.get_config().await;
        let source = session.thread_config_snapshot().await.session_source;
        let version = session
            .multi_agent_version()
            .unwrap_or_else(|| config.multi_agent_version_from_features());
        session
            .services
            .agent_control
            .check_turn_admission(version, &source)
            .map_err(|err| AttentionError::Unavailable(err.to_string()))?;
    }
    notice.admission = ThreadAttentionResponse::Held {
        reason: ThreadAttentionHeldReason::AdmissionUncertain,
    };
    let mut transaction = Transaction {
        state,
        settled: false,
    };
    transaction
        .state
        .notices
        .insert(notice.batch_id.clone(), notice.clone());
    // No admission is possible until the uncertain intent is confirmed durable.
    persist(live, &notice).await?;
    notice.admission = if blocking_request {
        ThreadAttentionResponse::Held {
            reason: ThreadAttentionHeldReason::BlockingRequest,
        }
    } else {
        admit(session, request, submission_id).await?
    };
    persist(live, &notice).await?;
    transaction
        .state
        .notices
        .insert(notice.batch_id.clone(), notice.clone());
    transaction.settled = true;
    drop(transaction);
    // A missing notification or reply cannot retract a durable final admission.
    session.deliver_attention_notice(notice.clone()).await;
    Ok(notice.admission)
}

async fn admit(
    session: &Arc<Session>,
    request: TurnInputRequest,
    submission_id: String,
) -> Result<ThreadAttentionResponse, AttentionError> {
    let TurnInput::ResponseItem(item) = request.input.clone() else {
        return Err(AttentionError::InvalidBinding(
            "attention must be a standalone tool notification".into(),
        ));
    };
    match session.inject_attention_if_running(item.clone()).await {
        Ok(turn_id) => return Ok(accepted(ThreadAttentionDelivery::Buffered, turn_id)),
        Err(ThreadAttentionHeldReason::BlockingRequest) => {
            return Ok(ThreadAttentionResponse::Held {
                reason: ThreadAttentionHeldReason::BlockingRequest,
            });
        }
        Err(_) => {}
    }
    // Invoke the existing handler directly. Enqueue-and-wait here would self-wait
    // on this same submission loop and can deadlock with shutdown.
    let result = turn_input::handle(
        session,
        WithTurnExtensionData::from(request),
        TurnInputMode::StartIfIdle,
        submission_id,
    )
    .await
    .map_err(|err| AttentionError::Unavailable(err.to_string()))?;
    Ok(match result {
        TurnInputSubmission::Started { turn_id, .. } => {
            accepted(ThreadAttentionDelivery::Started, turn_id)
        }
        TurnInputSubmission::NotSubmitted {
            reason: NotSubmittedReason::NotIdle,
        } => match session.inject_attention_if_running(item).await {
            Ok(turn_id) => accepted(ThreadAttentionDelivery::Buffered, turn_id),
            Err(reason) => ThreadAttentionResponse::Held { reason },
        },
        TurnInputSubmission::NotSubmitted { reason } => ThreadAttentionResponse::Held {
            reason: match reason {
                NotSubmittedReason::PlanMode => ThreadAttentionHeldReason::PlanMode,
                NotSubmittedReason::PendingTriggerTurn => {
                    ThreadAttentionHeldReason::PendingTriggerTurn
                }
                NotSubmittedReason::ServerDraining => ThreadAttentionHeldReason::ServerDraining,
                _ => ThreadAttentionHeldReason::Busy,
            },
        },
        TurnInputSubmission::Steered { .. } => {
            return Err(AttentionError::Unavailable(
                "quiet admission unexpectedly steered".into(),
            ));
        }
    })
}

fn accepted(delivery: ThreadAttentionDelivery, turn_id: String) -> ThreadAttentionResponse {
    ThreadAttentionResponse::Accepted {
        delivery,
        turn_id: Some(turn_id),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_protocol::attention::ThreadAttentionOrigin;
    use codex_protocol::attention::ThreadAttentionOriginKind;

    #[test]
    fn attention_notification_uses_standalone_tool_output_not_an_instruction_role() {
        let notice = notice(ThreadId::new(), "batch-one", "cue:one");
        let item = notification_item(&notice);
        let serialized = serde_json::to_value(&item).unwrap();
        assert_eq!(serialized["type"], "function_call_output");
        assert_eq!(serialized["name"], "attention");
        assert_eq!(serialized["namespace"], "observatory");
        assert!(serialized["call_id"].is_null());
        assert!(serialized.get("role").is_none());
        let output: serde_json::Value =
            serde_json::from_str(serialized["output"].as_str().unwrap()).unwrap();
        assert_eq!(output["thread_id"], notice.thread_id);
        assert_eq!(output["batch_id"], notice.batch_id);
        assert_eq!(output["origins"][0]["sourceRef"], "cue:one");
        assert!(output.get("admission").is_none());
    }

    fn notice(id: ThreadId, batch: &str, source: &str) -> ThreadAttentionNotification {
        ThreadAttentionNotification {
            thread_id: id.to_string(),
            batch_id: batch.into(),
            origins: vec![ThreadAttentionOrigin {
                kind: ThreadAttentionOriginKind::Peer,
                source_ref: source.into(),
                sender: None,
            }],
            admission: ThreadAttentionResponse::Held {
                reason: ThreadAttentionHeldReason::AdmissionUncertain,
            },
        }
    }
    #[test]
    fn canonical_attention_reducer_excludes_parent_and_rejects_contradictory_bindings() {
        let id = ThreadId::default();
        let parent = ThreadId::default();
        assert!(
            reduce(id, vec![notice(parent, "batch", "source")])
                .unwrap()
                .is_empty()
        );
        assert!(
            reduce(
                id,
                vec![
                    notice(id, "batch", "source"),
                    notice(id, "batch", "changed")
                ]
            )
            .is_err()
        );
        assert!(
            reduce(
                id,
                vec![notice(id, "batch", "source"), notice(id, "other", "source")]
            )
            .is_err()
        );
        let mut accepted_notice = notice(id, "batch", "source");
        accepted_notice.admission = accepted(ThreadAttentionDelivery::Started, "turn".into());
        assert!(
            reduce(
                id,
                vec![notice(id, "batch", "source"), accepted_notice.clone()]
            )
            .is_ok()
        );
        assert!(reduce(id, vec![accepted_notice, notice(id, "batch", "source")]).is_err());
    }
    #[tokio::test]
    async fn canonical_attention_failed_transaction_keeps_quarantine_and_refuses_listing() {
        let id = ThreadId::default();
        let ledger = AttentionLedger::default();
        let mut state = ledger.state.lock().await;
        state.readiness = Readiness::Ready;
        let intent = notice(id, "batch", "source");
        state
            .notices
            .insert(intent.batch_id.clone(), intent.clone());
        let transaction = Transaction {
            state,
            settled: false,
        };
        drop(transaction);
        let state = ledger.state.lock().await;
        assert!(quarantined(state.notices.get("batch").unwrap()));
        assert!(state.validate(&notice(id, "new", "source")).is_err());
        drop(state);
        assert!(ledger.snapshot(id, None).await.is_err());
    }
    #[tokio::test]
    async fn canonical_attention_absent_persistence_and_closed_lifecycle_never_return_empty() {
        let id = ThreadId::default();
        let ledger = AttentionLedger::default();
        assert!(ledger.snapshot(id, None).await.is_err());
        let closed = AttentionLedger::default();
        closed.close().await;
        assert!(matches!(
            closed.snapshot(id, None).await,
            Err(AttentionError::Stopped)
        ));
    }
    #[test]
    fn canonical_attention_preserved_cold_state_returns_same_key_and_refuses_cross_key() {
        let id = ThreadId::new();
        for uncertain in [false, true] {
            let mut held = notice(id, "z-held", "shared");
            held.admission = ThreadAttentionResponse::Held {
                reason: ThreadAttentionHeldReason::BlockingRequest,
            };
            let mut admitted = notice(id, "a-admitted", "shared");
            if !uncertain {
                admitted.admission = accepted(ThreadAttentionDelivery::Started, "turn".into());
            }
            let original = vec![held, admitted.clone()];
            let copy = codex_thread_store::attention_receipts_for_preservation(
                reduce(id, original.clone()).unwrap(),
            );
            let state = LedgerState {
                readiness: Readiness::Ready,
                closing: false,
                notices: reduce(id, [original, copy].concat()).unwrap(),
            };
            assert_eq!(
                state.validate(&admitted).unwrap(),
                Some(admitted.admission.clone())
            );
            assert!(state.validate(&notice(id, "another", "shared")).is_err());
            assert!(state.validate(&notice(id, "z-held", "changed")).is_err());
        }
    }
}
