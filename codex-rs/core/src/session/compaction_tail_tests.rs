//! Actual async-hook drain, turn-local claim, and post-await validity boundaries.
use super::*;
use pretty_assertions::assert_eq;

fn ready_hook_result(turn_id: &str) -> codex_protocol::protocol::HookCompletedEvent {
    serde_json::from_value(json!({
        "turn_id": turn_id,
        "run": {
            "id": "async-compaction-tail", "event_name": "user_prompt_submit",
            "handler_type": "command", "execution_mode": "async", "scope": "turn",
            "source_path": "/fixture/hooks.json", "source": "user", "display_order": 0,
            "status": "completed", "status_message": null,
            "started_at": 0, "completed_at": 1, "duration_ms": 1,
            "entries": [
                {"kind": "context", "text": "READY_HOOK_CONTEXT_FOR_COMPACTION"},
                {"kind": "warning", "text": "ready async operation was not blocked"}
            ]
        }
    }))
    .unwrap()
}

async fn prepare_tail(
    token_budget: bool,
    block_delivery: bool,
) -> (
    Arc<Session>,
    Arc<TurnContext>,
    async_channel::Receiver<Event>,
    CancellationToken,
) {
    let (mut session, turn, mut events) = make_session_and_context_with_auth_and_config_and_rx(
        CodexAuth::create_dummy_chatgpt_auth_for_testing(),
        Vec::new(),
        move |config| {
            if token_budget {
                config.features.enable(Feature::TokenBudget).unwrap();
            } else {
                config.features.disable(Feature::TokenBudget).unwrap();
            }
            config.model_provider.supports_websockets = true;
        },
    )
    .await;
    let (results, receiver) = async_channel::unbounded();
    results.try_send(ready_hook_result(&turn.sub_id)).unwrap();
    let mutable = Arc::get_mut(&mut session).unwrap();
    mutable.async_hook_results = receiver;
    if block_delivery {
        let (sender, receiver) = async_channel::bounded(1);
        mutable.tx_event = sender;
        events = receiver;
    }
    session
        .spawn_task(
            Arc::clone(&turn),
            Vec::new(),
            NeverEndingTask {
                kind: TaskKind::Regular,
                listen_to_cancellation_token: false,
            },
        )
        .await;
    let (_, token) = session
        .active_turn_context_and_cancellation_token()
        .await
        .unwrap();
    assert!(
        session
            .services
            .model_client
            .force_http_fallback(&turn.session_telemetry, turn.model_info())
    );
    if block_delivery {
        session.emit_turn_started(&turn, TaskKind::Regular).await;
    }
    (session, turn, events, token)
}

#[test_case::test_case(false, false, false; "ordinary queues without request")]
#[test_case::test_case(false, true, false; "ordinary summary records hook")]
#[test_case::test_case(true, true, false; "native reset retains result channel")]
#[test_case::test_case(true, true, true; "controlled native summary records hook")]
#[tokio::test]
async fn explicit_compaction_tail_uses_actual_mechanism_for_hook_context(
    token_budget: bool,
    request: bool,
    controls: bool,
) {
    let (session, turn, _events, token) = prepare_tail(token_budget, false).await;
    if request {
        assert!(
            session
                .request_context_compaction(
                    &turn.sub_id,
                    codex_protocol::protocol::CompactionInput {
                        instructions: controls.then(|| "preserve ready context".into()),
                        ..Default::default()
                    }
                )
                .await
        );
    }
    let claimed =
        super::super::turn::claim_compaction_and_drain_async_hooks(&session, &turn, &token)
            .await
            .unwrap();
    assert_eq!(claimed.is_some(), request);
    let records = format!(
        "{:?}",
        session
            .clone_history()
            .await
            .raw_items()
            .collect::<Vec<_>>()
    );
    let state = {
        let active = session.active_turn.lock().await;
        Arc::clone(&active.as_ref().unwrap().turn_state)
    };
    let queued = session
        .input_queue
        .take_pending_input_for_turn_state(&state)
        .await;
    let record_now = request && (!token_budget || controls);
    assert_eq!(
        records.matches("READY_HOOK_CONTEXT_FOR_COMPACTION").count(),
        usize::from(record_now)
    );
    assert_eq!(
        format!("{queued:?}")
            .matches("READY_HOOK_CONTEXT_FOR_COMPACTION")
            .count(),
        usize::from(!request)
    );
    assert_eq!(
        session.async_hook_results.len(),
        usize::from(request && token_budget && !controls)
    );
    assert!(
        session
            .take_context_compaction_request(&turn.sub_id)
            .await
            .is_none()
    );
}

#[test_case::test_case(false, false; "cancel during actual drain")]
#[test_case::test_case(true, false; "replace during actual drain")]
#[test_case::test_case(false, true; "cancel after drain before execution")]
#[test_case::test_case(true, true; "replace after drain before execution")]
#[tokio::test]
async fn explicit_compaction_tail_drops_claim_on_cancel_or_replacement(
    replace: bool,
    after_drain: bool,
) {
    let (session, turn, events, token) = prepare_tail(false, !after_drain).await;
    let before_window = session.current_window_id().await;
    let step = session
        .capture_step_context(Arc::clone(&turn), &token)
        .await
        .unwrap();
    assert!(
        session
            .request_context_compaction(
                &turn.sub_id,
                codex_protocol::protocol::CompactionInput {
                    retry_websocket: true,
                    ..Default::default()
                }
            )
            .await
    );
    let mut drain = Box::pin(tokio::task::unconstrained(
        super::super::turn::claim_compaction_and_drain_async_hooks(&session, &turn, &token),
    ));
    let claimed = if after_drain {
        Some(drain.as_mut().await.unwrap().unwrap())
    } else {
        assert!(futures::poll!(drain.as_mut()).is_pending());
        assert!(
            session
                .take_context_compaction_request(&turn.sub_id)
                .await
                .is_none(),
            "already claimed before actual event delivery blocks"
        );
        None
    };
    if replace {
        let (_, replacement, _) = make_session_and_context_with_rx().await;
        session
            .active_turn
            .lock()
            .await
            .as_mut()
            .unwrap()
            .task
            .as_mut()
            .unwrap()
            .turn_context = replacement;
    } else {
        session
            .active_turn
            .lock()
            .await
            .as_ref()
            .unwrap()
            .task
            .as_ref()
            .unwrap()
            .cancellation_token
            .cancel();
    }
    let collector = tokio::spawn(async move { while events.recv().await.is_ok() {} });
    let result = if let Some(claimed) = claimed {
        let mut client = session.services.model_client.new_session();
        super::super::turn::run_requested_compact(&session, step, &mut client, &claimed, &token)
            .await
    } else {
        tokio::time::timeout(std::time::Duration::from_secs(5), drain)
            .await
            .unwrap()
            .map(|_| true)
    };
    collector.abort();
    assert!(matches!(
        result.unwrap_err().details(),
        codex_protocol::error::CodexErrorDetails::TurnAborted
    ));
    assert_eq!(session.current_window_id().await, before_window);
    assert!(!session.services.model_client.responses_websocket_enabled());
    assert!(
        session
            .take_context_compaction_request(&turn.sub_id)
            .await
            .is_none()
    );
}
