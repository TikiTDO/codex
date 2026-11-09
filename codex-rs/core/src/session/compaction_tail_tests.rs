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

#[test_case::test_case(false, false; "cancel after reset completion before observed consume")]
#[test_case::test_case(true, false; "replace after reset completion before observed consume")]
#[test_case::test_case(false, true; "cancel during post-reset drain")]
#[test_case::test_case(true, true; "replace during post-reset drain")]
#[tokio::test]
async fn explicit_compaction_native_reset_fences_actual_result_consumption(
    replace: bool,
    during_drain: bool,
) {
    let (session, turn, events, token) = prepare_tail(true, true).await;
    let replacement = if replace {
        Some(make_session_and_context_with_rx().await.1)
    } else {
        None
    };
    let before_window = session.current_window_id().await;
    let step = session
        .capture_step_context(Arc::clone(&turn), &token)
        .await
        .unwrap();
    let input = codex_protocol::protocol::CompactionInput {
        retry_websocket: true,
        ..Default::default()
    };
    assert!(
        session
            .request_context_compaction(&turn.sub_id, input)
            .await
    );
    let claimed =
        super::super::turn::claim_compaction_and_drain_async_hooks(&session, &turn, &token)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(session.async_hook_results.len(), 1);
    let mut client = session.services.model_client.new_session();
    let mut reset = Box::pin(tokio::task::unconstrained(
        super::super::turn::run_requested_compact(&session, step, &mut client, &claimed, &token),
    ));
    // Only this manually driven reset produces TokenCount in this fixture. The
    // final recompute follows actual history replacement; window ID alone does not.
    let mut compaction_item_id = None;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            tokio::select! {
                result = reset.as_mut() => panic!("reset returned before its final token witness: {result:?}"),
                event = events.recv() => {
                    let event = event.unwrap();
                    if event.id == turn.sub_id
                        && let EventMsg::ItemStarted(started) = &event.msg
                        && started.turn_id == turn.sub_id
                        && matches!(started.item, TurnItem::ContextCompaction(_))
                    {
                        compaction_item_id = Some(started.item.id());
                    }
                    if event.id == turn.sub_id
                        && matches!(event.msg, EventMsg::TokenCount(_))
                        && session.current_window_id().await != before_window
                    {
                        break;
                    }
                }
            }
        }
    }).await.expect("actual reset token witness");
    let compaction_item_id = compaction_item_id.expect("exact invocation started item");
    let mut active = if during_drain {
        // Consume only modern completion for this invocation, then STOP reading.
        // Its real legacy ContextCompacted fills the capacity-one queue, so the
        // later mandatory async-hook Warning cannot complete its send.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                tokio::select! {
                    result = reset.as_mut() => panic!("reset returned before modern completion: {result:?}"),
                    event = events.recv() => {
                        let event = event.unwrap();
                        if event.id == turn.sub_id
                            && let EventMsg::ItemCompleted(completed) = event.msg
                            && completed.turn_id == turn.sub_id
                            && matches!(completed.item, TurnItem::ContextCompaction(_))
                            && completed.item.id() == compaction_item_id
                        {
                            break;
                        }
                    }
                }
            }
        }).await.expect("exact native reset modern completion");
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                assert!(
                    futures::poll!(reset.as_mut()).is_pending(),
                    "early Ready is a failed drain witness"
                );
                if session.async_hook_results.is_empty() {
                    assert_eq!(
                        events.len(),
                        1,
                        "retain actual legacy delivery backpressure"
                    );
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actual result receive while legacy delivery remains queued");
        session.active_turn.lock().await
    } else {
        let active = session.active_turn.lock().await;
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                tokio::select! {
                    result = reset.as_mut() => panic!("reset returned while owner held: {result:?}"),
                    event = events.recv() => {
                        let event = event.unwrap();
                        if event.id == turn.sub_id && matches!(event.msg, EventMsg::ContextCompacted(_)) {
                            break;
                        }
                    }
                }
            }
        }).await.expect("actual reset completion delivery");
        // Completion precedes post-hook preparation and inner return. This proves
        // cancellation after observed completion and before observed consumption,
        // NOT suspension at a measured post-inner-return/preconsume lock boundary.
        assert_eq!(
            session.async_hook_results.len(),
            1,
            "no consumption before cancellation"
        );
        active
    };
    let task = active.as_mut().unwrap().task.as_mut().unwrap();
    if let Some(replacement) = replacement {
        task.turn_context = replacement;
    } else {
        task.cancellation_token.cancel();
    }
    drop(active);
    let collector = tokio::spawn(async move { while events.recv().await.is_ok() {} });
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), reset)
        .await
        .unwrap();
    collector.abort();
    assert!(matches!(
        result.unwrap_err().details(),
        codex_protocol::error::CodexErrorDetails::TurnAborted
    ));
    assert_eq!(session.async_hook_results.len(), usize::from(!during_drain));
    let history = session.clone_history().await;
    assert_eq!(
        format!("{:?}", history.raw_items().collect::<Vec<_>>())
            .matches("READY_HOOK_CONTEXT_FOR_COMPACTION")
            .count(),
        usize::from(during_drain)
    );
    assert!(!session.services.model_client.responses_websocket_enabled());
}

#[tokio::test]
async fn explicit_compaction_native_reset_keeps_pending_user_and_mail_outside_hook_drain() {
    let (session, turn, _events, token) = prepare_tail(true, false).await;
    session
        .inject_if_running(vec![user_message("PENDING_ORDINARY_USER_INPUT")])
        .await
        .unwrap();
    let communication = codex_protocol::protocol::InterAgentCommunication::new(
        codex_protocol::AgentPath::root().join("worker").unwrap(),
        codex_protocol::AgentPath::root(),
        Vec::new(),
        "PENDING_ORDINARY_MAIL".into(),
        true,
    );
    session
        .input_queue
        .enqueue_mailbox_communication(communication.into(), Default::default())
        .await;
    let step = session
        .capture_step_context(Arc::clone(&turn), &token)
        .await
        .unwrap();
    let mut client = session.services.model_client.new_session();
    assert!(
        super::super::turn::run_requested_compact(
            &session,
            step,
            &mut client,
            &codex_protocol::protocol::CompactionInput::default(),
            &token
        )
        .await
        .unwrap()
    );
    let history = format!(
        "{:?}",
        session
            .clone_history()
            .await
            .raw_items()
            .collect::<Vec<_>>()
    );
    assert_eq!(
        history.matches("READY_HOOK_CONTEXT_FOR_COMPACTION").count(),
        1
    );
    assert!(!history.contains("PENDING_ORDINARY_USER_INPUT"));
    assert!(!history.contains("PENDING_ORDINARY_MAIL"));
    assert!(session.input_queue.has_trigger_turn_mailbox_items().await);
    let turn_state = Arc::clone(
        &session
            .active_turn
            .lock()
            .await
            .as_ref()
            .unwrap()
            .turn_state,
    );
    let pending = session
        .input_queue
        .take_pending_input_for_turn_state(&turn_state)
        .await;
    assert_eq!(
        format!("{pending:?}")
            .matches("PENDING_ORDINARY_USER_INPUT")
            .count(),
        1
    );
}

#[test_case::test_case(false; "native precompact stop")]
#[test_case::test_case(true; "native postcompact stop after install")]
#[tokio::test]
async fn explicit_compaction_native_reset_stop_leaves_ready_result_unconsumed(post: bool) {
    let (session, turn, _events, token) = prepare_tail(true, false).await;
    let root = tempfile::tempdir().unwrap();
    let root = codex_utils_absolute_path::AbsolutePathBuf::try_from(root.path()).unwrap();
    let event = if post { "PostCompact" } else { "PreCompact" };
    let hook_events: codex_config::HookEventsToml = serde_json::from_value(json!({
        event:[{"hooks":[{"type":"command", "command":"printf '%s\\n' '{\"continue\":false,\"stopReason\":\"native reset fixture stop\"}'", "timeout":10}]}]
    })).unwrap();
    let (hooks, _) = codex_hooks::Hooks::new(
        codex_hooks::HooksConfig {
            feature_enabled: true,
            bypass_hook_trust: true,
            shell_program: Some("/bin/sh".into()),
            shell_args: vec!["-c".into()],
            plugin_hook_sources: vec![codex_plugin::PluginHookSource {
                plugin_id: codex_plugin::PluginId::parse("reset-stop@test").unwrap(),
                plugin_root: root.clone(),
                plugin_data_root: root.join("data"),
                source_path: root.join("hooks.json"),
                source_relative_path: "hooks.json".into(),
                hooks: hook_events,
            }],
            ..Default::default()
        },
        session.thread_id,
        Arc::new(crate::hook_mcp_executor::CoreHookMcpExecutor {
            runtime: Arc::clone(&session.services.mcp_runtime),
            thread_id: session.thread_id,
        }),
    )
    .unwrap();
    session.services.hooks.store(Arc::new(hooks));
    let before = session.current_window_id().await;
    let step = session
        .capture_step_context(Arc::clone(&turn), &token)
        .await
        .unwrap();
    let mut client = session.services.model_client.new_session();
    let result = super::super::turn::run_requested_compact(
        &session,
        step,
        &mut client,
        &codex_protocol::protocol::CompactionInput {
            retry_websocket: true,
            ..Default::default()
        },
        &token,
    )
    .await;
    assert!(matches!(
        result.unwrap_err().details(),
        codex_protocol::error::CodexErrorDetails::TurnAborted
    ));
    assert_eq!(
        session.current_window_id().await != before,
        post,
        "post-stop is not rollback"
    );
    assert_eq!(session.async_hook_results.len(), 1);
    assert!(!session.services.model_client.responses_websocket_enabled());
}
