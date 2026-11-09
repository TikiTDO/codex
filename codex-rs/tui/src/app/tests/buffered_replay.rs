//! Real buffered notifications must use finalized rendering without losing live stream tails.

use super::*;

#[test]
fn quiet_attention_provenance_survives_buffer_eviction_but_not_foreground_input() {
    use codex_app_server_protocol::{
        AttentionTurnStartedNotification, TurnItemsView, TurnStartedNotification,
    };
    let mut store = ThreadEventStore::new(1);
    store.push_notification(ServerNotification::AttentionTurnStarted(
        AttentionTurnStartedNotification {
            thread_id: "thread".into(),
            turn_id: "background".into(),
        },
    ));
    store.push_notification(ServerNotification::TurnStarted(TurnStartedNotification {
        thread_id: "thread".into(),
        turn: Turn {
            id: "background".into(),
            root_turn_id: None,
            items: Vec::new(),
            items_view: TurnItemsView::Full,
            status: TurnStatus::InProgress,
            error: None,
            started_at: None,
            completed_at: None,
            duration_ms: None,
        },
    }));
    assert!(store.snapshot().events.iter().any(|event| matches!(event,
        ThreadBufferedEvent::Notification(n)
            if matches!(n.as_ref(), ServerNotification::AttentionTurnStarted(marker)
                if marker.turn_id == "background"))));
    store.push_notification(ServerNotification::ItemStarted(ItemStartedNotification {
        thread_id: "thread".into(),
        turn_id: "background".into(),
        started_at_ms: 0,
        item: ThreadItem::untimed(codex_app_server_protocol::ThreadItemPayload::UserMessage {
            id: "operator".into(),
            content: Vec::new(),
            client_id: None,
        }),
    }));
    assert!(!store.snapshot().events.iter().any(|event| matches!(event,
        ThreadBufferedEvent::Notification(n)
            if matches!(n.as_ref(), ServerNotification::AttentionTurnStarted(_)))));
}
use crate::chatwidget::tests::helpers::normalize_snapshot_paths;
use crate::chatwidget::tests::helpers::render_bottom_popup;
use codex_app_server_protocol::ItemCompletedNotification;
use codex_app_server_protocol::ItemStartedNotification;
use codex_app_server_protocol::ReasoningSummaryTextDeltaNotification;
use codex_app_server_protocol::ThreadItem;
use pretty_assertions::assert_eq;

fn delta(thread: &str, turn: &str, item: &str) -> ServerNotification {
    ServerNotification::AgentMessageDelta(AgentMessageDeltaNotification {
        thread_id: thread.into(),
        turn_id: turn.into(),
        item_id: item.into(),
        delta: "Partial **answer**\n".into(),
    })
}

fn completed(thread: &str) -> ServerNotification {
    ServerNotification::ItemCompleted(ItemCompletedNotification::new(
        thread.into(),
        "turn".into(),
        ThreadItemPayload::AgentMessage {
            id: "answer".into(),
            text: "Final **answer**".into(),
            phase: None,
            memory_citation: None,
            delivery: None,
            questions: None,
        },
        /*started_at_ms*/ None,
        /*completed_at_ms*/ 0,
    ))
}

#[tokio::test]
async fn refreshed_active_reasoning_accepts_later_deltas_and_complete_summary() {
    for (refresh, refreshed_item_is_partial, capacity, voice_handoff) in [
        (true, false, 1, false),
        (true, true, 1, false),
        (false, false, 8, false),
        (false, false, 1, false),
        (true, false, 1, true),
    ] {
        let (mut app, mut events, _ops) = make_test_app_with_channels().await;
        let thread_id = ThreadId::new();
        let session = test_thread_session(thread_id, app.config.cwd.to_path_buf());
        let mut turn = test_turn("reasoning-turn", TurnStatus::InProgress, Vec::new());
        let channel =
            ThreadEventChannel::new_with_session(capacity, session.clone(), vec![turn.clone()]);
        {
            let mut store = channel.store.lock().await;
            store.push_notification(ServerNotification::ItemStarted(
                ItemStartedNotification::new(
                    thread_id.to_string(),
                    turn.id.clone(),
                    ThreadItemPayload::Reasoning {
                        id: "reasoning".into(),
                        summary: Vec::new(),
                        content: Vec::new(),
                    },
                    /*started_at_ms*/ 0,
                ),
            ));
            store.push_notification(ServerNotification::ReasoningSummaryTextDelta(
                ReasoningSummaryTextDeltaNotification {
                    thread_id: thread_id.to_string(),
                    turn_id: turn.id.clone(),
                    item_id: "reasoning".into(),
                    delta: "**First heading**\nOriginal analysis paragraph".into(),
                    summary_index: 0,
                },
            ));
            assert_eq!(store.buffer.len(), capacity.min(/*other*/ 2));
            if voice_handoff {
                store.push_notification(ServerNotification::ItemStarted(ItemStartedNotification::new(thread_id.to_string(), turn.id.clone(), ThreadItemPayload::UserMessage {
                        id: "voice-handoff".into(),
                        client_id: None,
                        content: vec![AppServerUserInput::Text {
                            text: "<realtime_delegation><input>spoken follow-up</input></realtime_delegation>".into(),
                            text_elements: Vec::new(),
                        }],
                    }, /*started_at_ms*/ 0)));
            }
        }
        let mut snapshot = channel.store.lock().await.snapshot();
        app.thread_event_channels.insert(thread_id, channel);
        if refreshed_item_is_partial {
            turn.items
                .push(ThreadItem::untimed(ThreadItemPayload::Reasoning {
                    id: "reasoning".into(),
                    summary: vec!["**First heading**\nOriginal analysis paragraph".into()],
                    content: Vec::new(),
                }));
        }
        if refresh {
            app.apply_refreshed_snapshot_thread(
                thread_id,
                AppServerStartedThread {
                    session,
                    turns: vec![turn],
                    blocks_direct_input: false,
                    task_tools_available: false,
                },
                &mut snapshot,
            )
            .await;
            assert!(snapshot.events.is_empty());
        }
        assert!(snapshot.active_reasoning_item.is_some());
        app.replay_thread_snapshot(snapshot, /*resume_restored_queue*/ false);
        app.chat_widget.handle_server_notification(
            ServerNotification::ReasoningSummaryTextDelta(ReasoningSummaryTextDeltaNotification {
                thread_id: thread_id.to_string(),
                turn_id: "reasoning-turn".into(),
                item_id: "reasoning".into(),
                delta: "\n**Running checks**\nFinal paragraph".into(),
                summary_index: 0,
            }),
            /*replay_kind*/ None,
        );
        assert!(render_bottom_popup(&app.chat_widget, /*width*/ 80).contains("Final paragraph"));
        let completed = ServerNotification::ItemCompleted(ItemCompletedNotification::new(thread_id.to_string(), "reasoning-turn".into(), ThreadItemPayload::Reasoning {
                    id: "reasoning".into(),
                    summary: vec![
                        "**First heading**\nOriginal analysis paragraph\n**Running checks**\nFinal paragraph"
                            .into(),
                    ],
                    content: Vec::new(),
                }, /*started_at_ms*/ None, /*completed_at_ms*/ 0));
        let store = &app.thread_event_channels[&thread_id].store;
        store.lock().await.push_notification_ref(&completed);
        assert!(
            store
                .lock()
                .await
                .snapshot()
                .active_reasoning_item
                .is_none()
        );
        app.chat_widget
            .handle_server_notification(completed, /*replay_kind*/ None);
        app.chat_widget.handle_server_notification(
            ServerNotification::ReasoningSummaryTextDelta(ReasoningSummaryTextDeltaNotification {
                thread_id: thread_id.to_string(),
                turn_id: "reasoning-turn".into(),
                item_id: "reasoning".into(),
                delta: "\n**Stale heading**".into(),
                summary_index: 0,
            }),
            /*replay_kind*/ None,
        );
        assert!(!render_bottom_popup(&app.chat_widget, /*width*/ 80).contains("Stale heading"));
        let transcript = std::iter::from_fn(|| events.try_recv().ok())
            .filter_map(|event| match event {
                AppEvent::InsertHistoryCell(cell)
                    if cell
                        .as_any()
                        .is::<crate::history_cell::ReasoningSummaryCell>() =>
                {
                    Some(lines_to_single_string(&cell.transcript_lines(/*width*/ 80)))
                }
                _ => None,
            })
            .collect::<String>();
        insta::allow_duplicates! {
            insta::assert_snapshot!(transcript, @"
            • Original analysis paragraph
              Running checks
              Final paragraph
            ");
        }
    }
}

#[test]
fn buffered_replay_keeps_unfinished_and_unrelated_deltas_in_order() {
    let completion = completed("thread");
    let unfinished = delta("thread", "turn", "unfinished");
    let other_turn = delta("thread", "other-turn", "answer");
    let other_thread = delta("other-thread", "turn", "answer");
    let later_delta = delta("thread", "turn", "answer");
    let mut store = ThreadEventStore::new(/*capacity*/ 16);
    for event in [
        delta("thread", "turn", "answer"),
        unfinished.clone(),
        other_turn.clone(),
        other_thread.clone(),
        completion.clone(),
        later_delta.clone(),
    ] {
        store.push_notification(event);
    }
    let mut events = store.snapshot().events;
    replay_filter::omit_completed_agent_deltas(&mut events);
    let actual = events
        .into_iter()
        .map(|event| match event {
            ThreadBufferedEvent::Notification(notification) => *notification,
            other => panic!("unexpected event: {other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        serde_json::to_value(actual).unwrap(),
        serde_json::to_value(vec![
            unfinished,
            other_turn,
            other_thread,
            completion,
            later_delta
        ])
        .unwrap()
    );
    assert_eq!(store.snapshot().events.len(), 6);
}

#[test]
fn evicted_voice_delegation_marker_still_suppresses_private_replay() {
    let mut store = ThreadEventStore::new(/*capacity*/ 2);
    let voice_request = ThreadItemPayload::UserMessage {
        id: "user".into(),
        client_id: None,
        content: vec![codex_app_server_protocol::UserInput::Text {
            text: "<realtime_delegation><input>question</input></realtime_delegation>".into(),
            text_elements: Vec::new(),
        }],
    };
    store.push_notification(ServerNotification::ItemStarted(
        codex_app_server_protocol::ItemStartedNotification::new(
            "thread".into(),
            "voice-turn".into(),
            voice_request.clone(),
            /*started_at_ms*/ 0,
        ),
    ));
    store.push_notification(delta("thread", "voice-turn", "reasoning"));
    let private = ServerNotification::ItemCompleted(ItemCompletedNotification::new(
        "thread".into(),
        "voice-turn".into(),
        ThreadItemPayload::AgentMessage {
            id: "reasoning".into(),
            text: "[ANALYSIS] private".into(),
            phase: None,
            memory_citation: None,
            delivery: None,
            questions: None,
        },
        /*started_at_ms*/ None,
        /*completed_at_ms*/ 0,
    ));
    store.push_notification(private.clone());
    assert!(store.snapshot().events.is_empty());
    store.push_notification(delta("thread", "voice-turn", "unfinished"));
    assert!(store.snapshot().events.is_empty());
    assert_eq!(store.snapshot().delegated_turns, vec!["voice-turn"]);
    for notification in [
        ServerNotification::ReasoningSummaryPartAdded(
            codex_app_server_protocol::ReasoningSummaryPartAddedNotification {
                thread_id: "thread".into(),
                turn_id: "voice-turn".into(),
                item_id: "reasoning".into(),
                summary_index: 0,
            },
        ),
        ServerNotification::ReasoningSummaryTextDelta(
            codex_app_server_protocol::ReasoningSummaryTextDeltaNotification {
                thread_id: "thread".into(),
                turn_id: "voice-turn".into(),
                item_id: "reasoning".into(),
                delta: "private summary".into(),
                summary_index: 0,
            },
        ),
        ServerNotification::ReasoningTextDelta(
            codex_app_server_protocol::ReasoningTextDeltaNotification {
                thread_id: "thread".into(),
                turn_id: "voice-turn".into(),
                item_id: "reasoning".into(),
                delta: "private raw reasoning".into(),
                content_index: 0,
            },
        ),
        ServerNotification::ItemCompleted(ItemCompletedNotification::new(
            "thread".into(),
            "voice-turn".into(),
            ThreadItemPayload::Reasoning {
                id: "reasoning".into(),
                summary: vec!["private summary".into()],
                content: vec!["private raw reasoning".into()],
            },
            /*started_at_ms*/ None,
            /*completed_at_ms*/ 0,
        )),
    ] {
        store.push_notification(notification);
        assert!(store.snapshot().events.is_empty());
    }

    let mut resumed = ThreadEventStore::new(/*capacity*/ 1);
    resumed.set_turns(vec![Turn {
        id: "voice-turn".into(),
        root_turn_id: None,
        items: vec![ThreadItem::untimed(voice_request)],
        items_view: codex_app_server_protocol::TurnItemsView::Summary,
        status: TurnStatus::InProgress,
        error: None,
        started_at: None,
        completed_at: None,
        duration_ms: None,
    }]);
    resumed.push_notification(delta("thread", "voice-turn", "unfinished"));
    let mut newer = resumed.turns[0].clone();
    newer.id = "newer-turn".into();
    resumed.set_turns(vec![resumed.turns[0].clone(), newer]);
    assert!(resumed.snapshot().events.is_empty());
    resumed.push_notification(delta("thread", "newer-turn", "unfinished"));
    assert!(resumed.snapshot().events.is_empty());
    let mut ordinary = ThreadEventStore::new(/*capacity*/ 2);
    let ServerNotification::ItemCompleted(mut normal) = private else {
        unreachable!()
    };
    normal.turn_id = "normal-turn".into();
    ordinary.push_notification(ServerNotification::ItemCompleted(normal));
    assert_eq!(ordinary.snapshot().events.len(), 1);
}

#[test]
fn saved_voice_turn_suppresses_reasoning_without_hiding_typed_reasoning() {
    let reasoning = ThreadItemPayload::Reasoning {
        id: "reasoning".into(),
        summary: vec!["private summary".into()],
        content: vec!["private raw reasoning".into()],
    };
    let voice_request = ThreadItemPayload::UserMessage {
        id: "voice-request".into(),
        client_id: None,
        content: vec![codex_app_server_protocol::UserInput::Text {
            text: "<realtime_delegation><input>question</input></realtime_delegation>".into(),
            text_elements: Vec::new(),
        }],
    };
    let turn = |id: &str, items| Turn {
        id: id.into(),
        root_turn_id: None,
        items,
        items_view: codex_app_server_protocol::TurnItemsView::Full,
        status: TurnStatus::Completed,
        error: None,
        started_at: None,
        completed_at: None,
        duration_ms: None,
    };
    let mut store = ThreadEventStore::new(/*capacity*/ 4);
    store.set_turns(vec![
        turn(
            "voice",
            vec![
                ThreadItem::untimed(voice_request.clone()),
                ThreadItem::untimed(reasoning.clone()),
            ],
        ),
        turn("typed", vec![ThreadItem::untimed(reasoning.clone())]),
    ]);
    assert_eq!(
        store.snapshot().turns,
        vec![
            turn("voice", vec![ThreadItem::untimed(voice_request)]),
            turn("typed", vec![ThreadItem::untimed(reasoning)])
        ]
    );
}

#[test]
fn snapshot_keeps_typed_output_before_voice_handoff_in_the_same_turn() {
    let commentary = |id: &str, text: &str| ThreadItemPayload::AgentMessage {
        id: id.into(),
        text: text.into(),
        phase: Some(codex_protocol::models::MessagePhase::Commentary),
        questions: None,
        memory_citation: None,
        delivery: None,
    };
    let marker = ThreadItemPayload::UserMessage {
        id: "voice".into(),
        client_id: None,
        content: vec![codex_app_server_protocol::UserInput::Text {
            text: "<realtime_delegation><input>question</input></realtime_delegation>".into(),
            text_elements: Vec::new(),
        }],
    };
    let typed = commentary("typed", "Earlier typed commentary");
    let private = commentary("private", "Later voice-private commentary");
    let mut store = ThreadEventStore::new(/*capacity*/ 8);
    store.set_turns(vec![Turn {
        id: "shared".into(),
        root_turn_id: None,
        items: vec![
            ThreadItem::untimed(typed.clone()),
            ThreadItem::untimed(marker.clone()),
            ThreadItem::untimed(private.clone()),
        ],
        items_view: codex_app_server_protocol::TurnItemsView::Full,
        status: TurnStatus::InProgress,
        error: None,
        started_at: None,
        completed_at: None,
        duration_ms: None,
    }]);
    for item in [typed.clone(), marker.clone(), private] {
        store.push_notification(ServerNotification::ItemCompleted(
            ItemCompletedNotification::new(
                "thread".into(),
                "shared".into(),
                item,
                /*started_at_ms*/ None,
                /*completed_at_ms*/ 0,
            ),
        ));
    }
    let snapshot = store.snapshot();
    assert_eq!(
        snapshot.turns[0]
            .items
            .iter()
            .map(|entry| &entry.payload)
            .collect::<Vec<_>>(),
        [&typed, &marker]
    );
    let completed = snapshot
        .events
        .into_iter()
        .filter_map(|event| match event {
            ThreadBufferedEvent::Notification(notification) => match *notification {
                ServerNotification::ItemCompleted(item) => Some(item.item.payload),
                _ => None,
            },
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        completed,
        vec![typed, snapshot.turns[0].items[1].payload.clone()]
    );
}

#[test]
fn snapshot_keeps_typed_item_completed_after_voice_handoff() {
    let typed = ThreadItemPayload::AgentMessage {
        id: "typed".into(),
        text: "Typed commentary completed late".into(),
        phase: Some(codex_protocol::models::MessagePhase::Commentary),
        questions: None,
        memory_citation: None,
        delivery: None,
    };
    let private = ThreadItemPayload::AgentMessage {
        id: "private".into(),
        text: "Voice-private commentary".into(),
        phase: Some(codex_protocol::models::MessagePhase::Commentary),
        questions: None,
        memory_citation: None,
        delivery: None,
    };
    let marker = ThreadItemPayload::UserMessage {
        id: "voice".into(),
        client_id: None,
        content: vec![codex_app_server_protocol::UserInput::Text {
            text: "<realtime_delegation><input>question</input></realtime_delegation>".into(),
            text_elements: Vec::new(),
        }],
    };
    let mut store = ThreadEventStore::new(/*capacity*/ 8);
    for item in [typed.clone(), marker, private.clone()] {
        store.push_notification(ServerNotification::ItemStarted(
            codex_app_server_protocol::ItemStartedNotification::new(
                "thread".into(),
                "shared".into(),
                item,
                /*started_at_ms*/ 0,
            ),
        ));
    }
    for item in [typed.clone(), private] {
        store.push_notification(ServerNotification::ItemCompleted(
            ItemCompletedNotification::new(
                "thread".into(),
                "shared".into(),
                item,
                /*started_at_ms*/ None,
                /*completed_at_ms*/ 0,
            ),
        ));
    }
    let completed = store
        .snapshot()
        .events
        .into_iter()
        .filter_map(|event| match event {
            ThreadBufferedEvent::Notification(notification) => match *notification {
                ServerNotification::ItemCompleted(n) => Some(n.item.payload),
                _ => None,
            },
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(completed, vec![typed]);
}

#[tokio::test]
async fn evicted_voice_marker_survives_widget_snapshot_for_late_reasoning() {
    let thread_id = ThreadId::new();
    let mut store = ThreadEventStore::new(/*capacity*/ 1);
    store.push_notification(ServerNotification::ItemStarted(
        codex_app_server_protocol::ItemStartedNotification::new(
            thread_id.to_string(),
            "voice-turn".into(),
            ThreadItemPayload::UserMessage {
                id: "user".into(),
                client_id: None,
                content: vec![codex_app_server_protocol::UserInput::Text {
                    text: "<realtime_delegation><input>question</input></realtime_delegation>"
                        .into(),
                    text_elements: Vec::new(),
                }],
            },
            /*started_at_ms*/ 0,
        ),
    ));
    store.push_notification(delta(&thread_id.to_string(), "voice-turn", "private"));
    let snapshot = store.snapshot();
    assert!(snapshot.events.is_empty());
    assert!(snapshot.turns.is_empty());
    assert_eq!(snapshot.delegated_turns, vec!["voice-turn"]);

    let mut app = make_test_app().await;
    let (widget, _sender, mut events, _ops) =
        crate::chatwidget::tests::make_chatwidget_manual_with_sender().await;
    app.active_thread_id = Some(thread_id);
    app.replace_chat_widget(widget);
    app.replay_thread_snapshot(snapshot, /*resume_restored_queue*/ false);
    app.chat_widget.handle_server_notification(
        ServerNotification::ReasoningSummaryTextDelta(
            codex_app_server_protocol::ReasoningSummaryTextDeltaNotification {
                thread_id: thread_id.to_string(),
                turn_id: "voice-turn".into(),
                item_id: "late-reasoning".into(),
                delta: "private after switch".into(),
                summary_index: 0,
            },
        ),
        /*replay_kind*/ None,
    );
    app.chat_widget.handle_server_notification(
        ServerNotification::ItemCompleted(ItemCompletedNotification::new(
            thread_id.to_string(),
            "voice-turn".into(),
            ThreadItemPayload::Reasoning {
                id: "late-reasoning".into(),
                summary: vec!["private after switch".into()],
                content: Vec::new(),
            },
            /*started_at_ms*/ None,
            /*completed_at_ms*/ 0,
        )),
        /*replay_kind*/ None,
    );
    app.chat_widget.handle_server_notification(
        delta(&thread_id.to_string(), "voice-turn", "private-commentary"),
        /*replay_kind*/ None,
    );
    app.chat_widget.handle_server_notification(
        ServerNotification::ItemCompleted(ItemCompletedNotification::new(
            thread_id.to_string(),
            "voice-turn".into(),
            ThreadItemPayload::AgentMessage {
                id: "private-commentary".into(),
                text: "[COMMENTARY] private after switch".into(),
                phase: None,
                memory_citation: None,
                delivery: None,
                questions: None,
            },
            /*started_at_ms*/ None,
            /*completed_at_ms*/ 0,
        )),
        /*replay_kind*/ None,
    );
    while let Ok(event) = events.try_recv() {
        if let AppEvent::InsertHistoryCell(cell) = event {
            let rendered = cell
                .transcript_lines(/*width*/ 80)
                .into_iter()
                .map(|line| line.to_string())
                .collect::<String>();
            assert!(!rendered.contains("private"), "{rendered}");
        }
    }
}

#[tokio::test]
async fn buffered_replay_renders_completed_text_without_streaming_again() {
    let (mut app, mut events, _ops) = make_test_app_with_channels().await;
    let mut store = ThreadEventStore::new(/*capacity*/ 16);
    store.push_notification(delta("thread", "turn", "answer"));
    store.push_notification(completed("thread"));
    app.replay_thread_snapshot(store.snapshot(), /*resume_restored_queue*/ false);
    let mut lines = Vec::new();
    while let Ok(event) = events.try_recv() {
        match event {
            AppEvent::InsertHistoryCell(cell) => lines.extend(cell.display_lines(/*width*/ 80)),
            AppEvent::StartCommitAnimation | AppEvent::ConsolidateAgentMessage { .. } => {
                panic!("completed replay must not reconstruct a stream")
            }
            _ => {}
        }
    }
    let text = lines
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    insta::assert_snapshot!(text.trim(), @"
    • Final answer
    ");
}

#[test]
fn buffered_replay_preserves_text_before_interactive_requests() {
    let mut store = ThreadEventStore::new(/*capacity*/ 16);
    store.push_notification(delta("thread", "turn", "answer"));
    store.push_request(
        codex_app_server_protocol::ServerRequest::ToolRequestUserInput {
            request_id: codex_app_server_protocol::RequestId::Integer(1),
            params: codex_app_server_protocol::ToolRequestUserInputParams {
                thread_id: "thread".into(),
                turn_id: "turn".into(),
                item_id: "tool".into(),
                questions: Vec::new(),
                is_blocking: true,
                auto_resolution_ms: None,
            },
        },
    );
    store.push_notification(completed("thread"));
    let mut events = store.snapshot().events;
    let before = format!("{events:?}");
    replay_filter::omit_completed_agent_deltas(&mut events);
    assert_eq!(format!("{events:?}"), before);
}

#[tokio::test]
async fn misalignment_buffered_replay_preserves_input_after_continuation() {
    let (mut app, mut history, _) = make_test_app_with_channels().await;
    let thread_id = ThreadId::new();
    app.chat_widget
        .set_queue_autosend_suppressed(/*suppressed*/ true);
    app.chat_widget.insert_str("keep queued");
    app.chat_widget
        .handle_key_event(KeyEvent::from(KeyCode::Tab));
    app.chat_widget
        .restore_user_message_to_composer("keep draft".into());
    let saved_input = app.chat_widget.capture_thread_input_state();
    let error = AppServerTurnError {
        misalignment: None,
        message: "Chat stopped".into(),
        codex_error_info: Some(AppServerCodexErrorInfo::MisalignmentPolicyViolation),
        additional_details: None,
    };
    let failed_turn = Turn {
        error: Some(error.clone()),
        items: vec![ThreadItem::untimed(ThreadItemPayload::McpToolCall {
            id: "tool-call".into(),
            server: "sample".into(),
            tool: "inspect".into(),
            status: codex_app_server_protocol::McpToolCallStatus::InProgress,
            arguments: serde_json::json!({}),
            app_context: None,
            mcp_app_resource_uri: None,
            mcp_app_ui: None,
            plugin_id: None,
            read_only_hint: None,
            result: None,
            error: None,
            duration_ms: None,
        })],
        ..test_turn("failed-turn", TurnStatus::Failed, Vec::new())
    };
    let continued_turn = test_turn("continued-turn", TurnStatus::Completed, Vec::new());
    // Primary resume also replays stored turns without going through the snapshot filter.
    app.chat_widget
        .set_queue_autosend_suppressed(/*suppressed*/ true);
    app.chat_widget.replay_thread_turns(
        vec![failed_turn.clone(), continued_turn.clone()],
        ReplayKind::ResumeInitialMessages,
    );
    let rendered = std::iter::from_fn(|| history.try_recv().ok())
        .filter_map(|event| match event {
            AppEvent::InsertHistoryCell(cell) => {
                Some(lines_to_single_string(&cell.display_lines(/*width*/ 80)))
            }
            _ => None,
        })
        .collect::<String>();
    insta::assert_snapshot!("misalignment_replayed_failed_tool", rendered);
    assert!(!app.chat_widget.has_misalignment_policy_violation());
    insta::assert_snapshot!(
        "misalignment_replayed_input",
        normalize_snapshot_paths(render_bottom_popup(&app.chat_widget, /*width*/ 80))
    );
    assert_eq!(app.chat_widget.composer_text_with_pending(), "keep draft");
    assert_eq!(
        app.chat_widget.queued_user_message_texts(),
        vec!["keep queued".to_string()]
    );
    let events = [
        turn_started_notification(thread_id, "failed-turn"),
        ServerNotification::Error(codex_app_server_protocol::ErrorNotification {
            thread_id: thread_id.to_string(),
            turn_id: "failed-turn".to_string(),
            error,
            will_retry: false,
        }),
        ServerNotification::TurnCompleted(TurnCompletedNotification {
            thread_id: thread_id.to_string(),
            turn: failed_turn.clone(),
        }),
        turn_started_notification(thread_id, "continued-turn"),
        ServerNotification::TurnCompleted(TurnCompletedNotification {
            thread_id: thread_id.to_string(),
            turn: continued_turn,
        }),
    ];
    // Retain the whole stream, only the new turn's delta, or only a post-completion notice.
    for (capacity, event_count) in [(16, 5), (1, 4), (1, 5)] {
        let mut store = ThreadEventStore::new_with_session(
            capacity,
            test_thread_session(thread_id, app.config.cwd.to_path_buf()),
            vec![failed_turn.clone()],
        );
        store.input_state = saved_input.clone();
        for event in &events[..event_count] {
            store.push_notification_ref(event);
        }
        if capacity == 1 {
            store.push_notification(if event_count == 4 {
                delta(&thread_id.to_string(), "continued-turn", "answer")
            } else {
                ServerNotification::Warning(WarningNotification {
                    message: "An unrelated notice".into(),
                    thread_id: Some(thread_id.to_string()),
                })
            });
            assert_eq!(store.buffer.len(), 1);
        }
        app.replay_thread_snapshot(store.snapshot(), /*resume_restored_queue*/ false);
        assert!(!app.chat_widget.has_misalignment_policy_violation());
        assert_eq!(app.chat_widget.composer_text_with_pending(), "keep draft");
        assert_eq!(
            app.chat_widget.queued_user_message_texts(),
            vec!["keep queued".to_string()]
        );
    }
}

#[tokio::test]
async fn misalignment_replay_blocks_when_turn_start_was_evicted() {
    let thread_id = ThreadId::new();
    let error = AppServerTurnError {
        misalignment: None,
        message: "Chat stopped".into(),
        codex_error_info: Some(AppServerCodexErrorInfo::MisalignmentPolicyViolation),
        additional_details: None,
    };
    let previous_turn = test_turn("old-turn", TurnStatus::Completed, Vec::new());
    let failed_turn = Turn {
        error: Some(error.clone()),
        ..test_turn("new-turn", TurnStatus::Failed, Vec::new())
    };
    for (stored_turn, notification) in [
        (
            previous_turn.clone(),
            ServerNotification::Error(codex_app_server_protocol::ErrorNotification {
                thread_id: thread_id.to_string(),
                turn_id: "new-turn".into(),
                error: error.clone(),
                will_retry: false,
            }),
        ),
        (
            previous_turn,
            ServerNotification::TurnCompleted(TurnCompletedNotification {
                thread_id: thread_id.to_string(),
                turn: failed_turn.clone(),
            }),
        ),
        // A settings-operation error has a submission ID, not evidence of a new turn.
        (
            failed_turn,
            ServerNotification::Error(codex_app_server_protocol::ErrorNotification {
                thread_id: thread_id.to_string(),
                turn_id: "settings-update".into(),
                error: AppServerTurnError {
                    codex_error_info: Some(AppServerCodexErrorInfo::BadRequest),
                    ..error
                },
                will_retry: false,
            }),
        ),
    ] {
        for saw_start in [false, true] {
            let mut app = make_test_app().await;
            let mut store = ThreadEventStore::new_with_session(
                /*capacity*/ 1,
                test_thread_session(thread_id, app.config.cwd.to_path_buf()),
                vec![stored_turn.clone()],
            );
            if saw_start {
                store.push_notification(turn_started_notification(thread_id, "new-turn"));
            }
            store.push_notification_ref(&notification);
            assert_eq!(store.buffer.len(), 1);
            app.replay_thread_snapshot(store.snapshot(), /*resume_restored_queue*/ false);
            assert!(app.chat_widget.has_misalignment_policy_violation());
            assert!(render_bottom_popup(&app.chat_widget, /*width*/ 80).contains("Chat stopped"));
        }
    }
}

fn attention_marker(thread_id: ThreadId, turn_id: &str) -> ServerNotification {
    ServerNotification::AttentionTurnStarted(
        codex_app_server_protocol::AttentionTurnStartedNotification {
            thread_id: thread_id.to_string(),
            turn_id: turn_id.into(),
        },
    )
}

fn snapshot_has_attention_marker(snapshot: &ThreadEventSnapshot, turn_id: &str) -> bool {
    snapshot.events.iter().any(|event| {
        matches!(event,
        ThreadBufferedEvent::Notification(n) if matches!(n.as_ref(),
            ServerNotification::AttentionTurnStarted(marker) if marker.turn_id == turn_id))
    })
}

#[test]
fn quiet_attention_native_horizon_prunes_on_turn_refresh_and_request_eviction() {
    let thread = ThreadId::new();
    let mut store = ThreadEventStore::new(1);
    store.push_notification(attention_marker(thread, "background"));
    store.push_notification(ServerNotification::TurnStarted(
        codex_app_server_protocol::TurnStartedNotification {
            thread_id: thread.to_string(),
            turn: test_turn("background", TurnStatus::InProgress, Vec::new()),
        },
    ));
    // Active provenance outlives arbitrary unrelated bounded churn.
    for _ in 0..10 {
        store.push_notification(delta(&thread.to_string(), "other", "item"));
        assert!(snapshot_has_attention_marker(
            &store.snapshot(),
            "background"
        ));
    }
    store.push_notification(ServerNotification::TurnCompleted(
        codex_app_server_protocol::TurnCompletedNotification {
            thread_id: thread.to_string(),
            turn: test_turn("background", TurnStatus::Completed, Vec::new()),
        },
    ));
    assert!(snapshot_has_attention_marker(
        &store.snapshot(),
        "background"
    ));
    store.set_turns(vec![test_turn(
        "background",
        TurnStatus::Completed,
        Vec::new(),
    )]);
    store.push_request(ServerRequest::ToolRequestUserInput {
        request_id: codex_app_server_protocol::RequestId::Integer(1),
        params: codex_app_server_protocol::ToolRequestUserInputParams {
            thread_id: thread.to_string(),
            turn_id: "other".into(),
            item_id: "question".into(),
            questions: Vec::new(),
            is_blocking: true,
            auto_resolution_ms: None,
        },
    });
    assert!(
        snapshot_has_attention_marker(&store.snapshot(), "background"),
        "native turns still retain it"
    );
    store.set_turns(Vec::new());
    assert!(
        !snapshot_has_attention_marker(&store.snapshot(), "background"),
        "request eviction and turn replacement removed every reference"
    );
}

#[tokio::test]
async fn quiet_attention_refresh_genuine_user_dominates_retained_raw_markers() {
    let (mut app, _events, _ops) = make_test_app_with_channels().await;
    let thread = ThreadId::new();
    let session = test_thread_session(thread, app.config.cwd.to_path_buf());
    app.chat_widget.handle_thread_session_quiet(session.clone());
    app.chat_widget
        .handle_server_notification(attention_marker(thread, "foreground"), None);
    assert!(
        app.chat_widget
            .is_background_attention_turn_for_test("foreground")
    );
    let channel = ThreadEventChannel::new_with_session(8, session.clone(), Vec::new());
    let mut snapshot = {
        let mut store = channel.store.lock().await;
        store.push_notification(attention_marker(thread, "foreground"));
        store.snapshot()
    };
    app.thread_event_channels.insert(thread, channel);
    let user = ThreadItemPayload::UserMessage {
        id: "operator".into(),
        content: Vec::new(),
        client_id: None,
    };
    app.apply_refreshed_snapshot_thread(
        thread,
        AppServerStartedThread {
            session,
            turns: vec![test_turn("foreground", TurnStatus::Completed, vec![user])],
            blocks_direct_input: false,
            task_tools_available: false,
        },
        &mut snapshot,
    )
    .await;
    assert!(!snapshot_has_attention_marker(&snapshot, "foreground"));
    assert!(!snapshot_has_attention_marker(
        &app.thread_event_channels[&thread]
            .store
            .lock()
            .await
            .snapshot(),
        "foreground"
    ));
    app.replay_thread_snapshot(snapshot, false);
    assert!(
        !app.chat_widget
            .is_background_attention_turn_for_test("foreground")
    );
}

#[tokio::test]
async fn quiet_attention_rebuilt_widget_retains_provenance_questions_and_later_live_completion() {
    use codex_app_server_protocol::{
        ErrorNotification, TurnCompletedNotification, TurnError, TurnStartedNotification,
    };
    for replay_terminal in [false, true] {
        for status in [
            TurnStatus::Completed,
            TurnStatus::Interrupted,
            TurnStatus::Failed,
        ] {
            let (mut original, _events, _ops) = make_test_app_with_channels().await;
            let thread = ThreadId::new();
            let session = test_thread_session(thread, original.config.cwd.to_path_buf());
            original
                .chat_widget
                .handle_thread_session_quiet(session.clone());
            original.chat_widget.handle_paste("main draft".into());
            original.chat_widget.handle_server_notification(
                ServerNotification::ItemCompleted(ItemCompletedNotification::new(
                    thread.to_string(),
                    "original".into(),
                    ThreadItemPayload::AgentMessage {
                        id: "original-question".into(),
                        text: "Question".into(),
                        phase: None,
                        memory_citation: None,
                        delivery: None,
                        questions: Some(vec![codex_protocol::items::AsyncUserInputQuestion {
                            title: "Which way?".into(),
                            options: None,
                        }]),
                    },
                    None,
                    0,
                )),
                None,
            );
            original
                .chat_widget
                .handle_key_event(KeyEvent::new(KeyCode::Up, KeyModifiers::ALT));
            original
                .chat_widget
                .handle_key_event(KeyEvent::from(KeyCode::Enter));
            original.chat_widget.handle_paste("retained answer".into());
            let input = original.chat_widget.capture_thread_input_state().unwrap();
            let questions = input.questions.clone();
            assert!(questions.is_some());
            let mut turn = test_turn("background", TurnStatus::InProgress, Vec::new());
            let mut store = ThreadEventStore::new_with_session(1, session, Vec::new());
            store.input_state = Some(input);
            store.push_notification(attention_marker(thread, &turn.id));
            store.push_notification(attention_marker(thread, &turn.id)); // duplicate is idempotent
            store.push_notification(ServerNotification::TurnStarted(TurnStartedNotification {
                thread_id: thread.to_string(),
                turn: turn.clone(),
            }));
            store.push_notification(delta(&thread.to_string(), &turn.id, "tail"));
            if replay_terminal {
                turn.status = status.clone();
                store.push_notification(ServerNotification::TurnCompleted(
                    TurnCompletedNotification {
                        thread_id: thread.to_string(),
                        turn: turn.clone(),
                    },
                ));
            }
            store.set_turns(vec![turn.clone()]);
            let snapshot = store.snapshot();
            assert!(
                matches!(&snapshot.events[0], ThreadBufferedEvent::Notification(n) if matches!(n.as_ref(), ServerNotification::AttentionTurnStarted(_))),
                "trusted marker must precede replay events independently of question suppression"
            );
            let (mut rebuilt, _events, _ops) = make_test_app_with_channels().await;
            rebuilt.replay_thread_snapshot(snapshot, false);
            assert!(
                rebuilt
                    .chat_widget
                    .is_background_attention_turn_for_test(&turn.id)
            );
            assert_eq!(
                rebuilt
                    .chat_widget
                    .capture_thread_input_state()
                    .unwrap()
                    .questions,
                questions
            );
            if !replay_terminal {
                turn.status = status;
                if turn.status == TurnStatus::Failed {
                    rebuilt.chat_widget.handle_server_notification(
                        ServerNotification::Error(ErrorNotification {
                            thread_id: thread.to_string(),
                            turn_id: turn.id.clone(),
                            will_retry: false,
                            error: TurnError {
                                message: "fixture failure".into(),
                                codex_error_info: None,
                                additional_details: None,
                                misalignment: None,
                            },
                        }),
                        None,
                    );
                    assert_eq!(
                        rebuilt
                            .chat_widget
                            .capture_thread_input_state()
                            .unwrap()
                            .questions,
                        questions
                    );
                }
                rebuilt.chat_widget.handle_server_notification(
                    ServerNotification::TurnCompleted(TurnCompletedNotification {
                        thread_id: thread.to_string(),
                        turn: turn.clone(),
                    }),
                    None,
                );
            }
            assert_eq!(
                rebuilt
                    .chat_widget
                    .capture_thread_input_state()
                    .unwrap()
                    .questions,
                questions
            );
            assert_eq!(
                rebuilt.chat_widget.composer_text_with_pending(),
                "main draft"
            );
            rebuilt.chat_widget.handle_server_notification(
                attention_marker(ThreadId::new(), "wrong-thread"),
                None,
            );
            assert!(
                !rebuilt
                    .chat_widget
                    .is_background_attention_turn_for_test("wrong-thread")
            );
        }
    }
}

#[test]
fn quiet_attention_rebase_error_reachability_and_multiple_native_turns_are_not_capacity_capped() {
    let thread = ThreadId::new();
    let mut store = ThreadEventStore::new(1);
    store.push_notification(attention_marker(thread, "a"));
    store.push_notification(ServerNotification::TurnStarted(
        codex_app_server_protocol::TurnStartedNotification {
            thread_id: thread.to_string(),
            turn: test_turn("a", TurnStatus::InProgress, Vec::new()),
        },
    ));
    store.clear_active_turn_id();
    store.push_notification(ServerNotification::Error(
        codex_app_server_protocol::ErrorNotification {
            thread_id: thread.to_string(),
            turn_id: "a".into(),
            will_retry: false,
            error: codex_app_server_protocol::TurnError {
                message: "fixture terminal error".into(),
                codex_error_info: None,
                additional_details: None,
                misalignment: None,
            },
        },
    ));
    assert!(
        snapshot_has_attention_marker(&store.snapshot(), "a"),
        "nonretry error is native replay reachability even without active identity"
    );
    store.rebase_buffer_after_session_refresh();
    assert!(
        !snapshot_has_attention_marker(&store.snapshot(), "a"),
        "rebase removed the last native reference"
    );
    let mut turns = Vec::new();
    for turn_id in ["a", "b", "c"] {
        store.push_notification(attention_marker(thread, turn_id));
        turns.push(test_turn(turn_id, TurnStatus::Completed, Vec::new()));
        store.set_turns(turns.clone());
    }
    for turn_id in ["a", "b", "c"] {
        assert!(
            snapshot_has_attention_marker(&store.snapshot(), turn_id),
            "native turns, not buffer capacity, bound retained provenance"
        );
    }
}


fn quiet_attention_native_user() -> ThreadItemPayload {
    ThreadItemPayload::UserMessage { id: "genuine-user".into(), content: Vec::new(), client_id: None }
}

#[test]
fn quiet_attention_late_marker_foreground_survives_raw_eviction_and_native_horizon() {
    for marker_first in [false, true] {
        let thread = ThreadId::new();
        let mut store = ThreadEventStore::new(1);
        if marker_first { store.push_notification(attention_marker(thread, "turn")); }
        store.push_notification(ServerNotification::TurnStarted(codex_app_server_protocol::TurnStartedNotification {
            thread_id: thread.to_string(), turn: test_turn("turn", TurnStatus::InProgress, Vec::new()),
        }));
        store.push_notification(ServerNotification::ItemCompleted(ItemCompletedNotification::new(
            thread.to_string(), "turn".into(), quiet_attention_native_user(), None, 0,
        )));
        store.push_notification(delta(&thread.to_string(), "unrelated", "tail"));
        assert!(!store.buffer.iter().any(|event| matches!(event,
            ThreadBufferedEvent::Notification(n) if matches!(n.as_ref(), ServerNotification::ItemCompleted(_)))));
        let marker = attention_marker(thread, "turn");
        assert!(store.suppresses_attention_marker(&marker));
        store.push_notification(marker.clone());
        assert!(!snapshot_has_attention_marker(&store.snapshot(), "turn"));
        store.set_turns(vec![test_turn("turn", TurnStatus::Completed, Vec::new())]);
        store.rebase_buffer_after_session_refresh();
        store.push_request(ServerRequest::ToolRequestUserInput {
            request_id: codex_app_server_protocol::RequestId::Integer(42),
            params: codex_app_server_protocol::ToolRequestUserInputParams {
                thread_id: thread.to_string(), turn_id: "other".into(), item_id: "question".into(),
                questions: Vec::new(), is_blocking: true, auto_resolution_ms: None,
            },
        });
        assert!(store.suppresses_attention_marker(&marker), "native turn remains reachable");
        store.set_turns(Vec::new());
        assert!(!store.suppresses_attention_marker(&marker), "actual horizon loss must prune foreground");
        store.push_notification(marker);
        assert!(snapshot_has_attention_marker(&store.snapshot(), "turn"));
    }
}

fn quiet_attention_edit_question(widget: &mut ChatWidget, thread: ThreadId) {
    widget.handle_paste("main draft".into());
    widget.handle_server_notification(ServerNotification::ItemCompleted(ItemCompletedNotification::new(
        thread.to_string(), "question-turn".into(), ThreadItemPayload::AgentMessage {
            id: "original-question".into(), text: "Question".into(), phase: None, memory_citation: None,
            delivery: None, questions: Some(vec![codex_protocol::items::AsyncUserInputQuestion {
                title: "Which way?".into(), options: None,
            }]),
        }, None, 0,
    )), None);
    widget.handle_key_event(KeyEvent::new(KeyCode::Up, KeyModifiers::ALT));
    widget.handle_key_event(KeyEvent::from(KeyCode::Enter));
    widget.handle_paste("retained answer".into());
    assert!(widget.capture_thread_input_state().unwrap().questions.is_some());
}

#[tokio::test]
async fn quiet_attention_actual_initial_replay_then_late_marker_keeps_foreground_cleanup() -> Result<()> {
    for terminal in 0..4 {
        // Compare the actual late-marker route to ordinary foreground, not a
        // weakened expectation derived from replay-only draft suppression.
        let mut ordinary = None;
        for late_marker in [false, true] {
            let (mut app, _events, _ops) = make_test_app_with_channels().await;
            let thread = ThreadId::new();
            let turn = test_turn("foreground", TurnStatus::InProgress, vec![quiet_attention_native_user()]);
            let session = test_thread_session(thread, app.config.cwd.to_path_buf());
            app.enqueue_primary_thread_session(session, vec![turn.clone()]).await?;
            quiet_attention_edit_question(&mut app.chat_widget, thread);
            let before_questions = app.chat_widget.capture_thread_input_state().unwrap().questions;
            if late_marker {
                app.enqueue_thread_notification(thread, attention_marker(thread, &turn.id)).await?;
                assert!(!snapshot_has_attention_marker(&app.thread_event_channels[&thread].store.lock().await.snapshot(), &turn.id));
                assert!(!app.chat_widget.is_background_attention_turn_for_test(&turn.id));
            }
            let event = if terminal == 3 {
                ServerNotification::Error(codex_app_server_protocol::ErrorNotification {
                    thread_id: thread.to_string(), turn_id: turn.id.clone(), will_retry: false,
                    error: codex_app_server_protocol::TurnError { message: "fixture failure".into(), codex_error_info: None, additional_details: None, misalignment: None },
                })
            } else {
                let mut turn = turn;
                turn.status = [TurnStatus::Completed, TurnStatus::Interrupted, TurnStatus::Failed][terminal].clone();
                ServerNotification::TurnCompleted(codex_app_server_protocol::TurnCompletedNotification { thread_id: thread.to_string(), turn })
            };
            app.enqueue_thread_notification(thread, event).await?;
            // FIFO delivery of the real following event proves a stale marker
            // was not placed before it on the active channel.
            let delivered = app.active_thread_rx.as_mut().unwrap().recv().await.unwrap();
            assert!(!matches!(&delivered, ThreadBufferedEvent::Notification(n) if matches!(n.as_ref(), ServerNotification::AttentionTurnStarted(_))));
            app.handle_thread_event_now_recovering_file_changes(delivered).await;
            assert!(!app.chat_widget.is_background_attention_turn_for_test("foreground"));
            let state = (app.chat_widget.capture_thread_input_state().unwrap().questions,
                app.chat_widget.composer_text_with_pending());
            // The native editor allocation remains Some. Pending question state
            // must change and its actual answer must reach the ordinary composer.
            assert_ne!(state.0, before_questions, "ordinary live foreground must settle pending question drafts");
            assert!(state.1.contains("retained answer"));
            if let Some(expected) = &ordinary { assert_eq!(&state, expected); } else { ordinary = Some(state); }
        }
    }
    Ok(())
}

#[tokio::test]
async fn quiet_attention_common_gate_reaches_true_background_but_not_parked_foreground_voice() -> Result<()> {
    for foreground in [false, true] {
        let (mut app, _events, _ops) = make_test_app_with_channels().await;
        let thread = ThreadId::new();
        let session = test_thread_session(thread, app.config.cwd.to_path_buf());
        app.chat_widget.handle_thread_session_quiet(session.clone());
        crate::chatwidget::activate_voice_for_thread(&mut app.chat_widget, thread);
        app.chat_widget.park_voice();
        let (visible, _, _, _) = crate::chatwidget::tests::make_chatwidget_manual_with_sender().await;
        app.background_voice = Some(Box::new(std::mem::replace(&mut app.chat_widget, visible)));
        let turn = test_turn("voice-turn", TurnStatus::InProgress,
            if foreground { vec![quiet_attention_native_user()] } else { Vec::new() });
        app.thread_event_channels.insert(thread, ThreadEventChannel::new_with_session(1, session, vec![turn]));
        app.enqueue_thread_notification(thread, attention_marker(thread, "voice-turn")).await?;
        let owner = app.background_voice.as_ref().expect("parked voice remains live");
        assert_eq!(owner.is_background_attention_turn_for_test("voice-turn"), !foreground);
        assert_eq!(snapshot_has_attention_marker(&app.thread_event_channels[&thread].store.lock().await.snapshot(), "voice-turn"), !foreground);
        app.enqueue_thread_notification(thread, attention_marker(ThreadId::new(), "wrong-thread")).await?;
        assert!(!app.background_voice.as_ref().unwrap().is_background_attention_turn_for_test("wrong-thread"));
    }
    Ok(())
}

#[tokio::test]
async fn quiet_attention_actual_background_initial_resume_keeps_late_marker_and_live_drafts() -> Result<()> {
    for terminal in 0..4 {
        let (mut app, _events, _ops) = make_test_app_with_channels().await;
        let thread = ThreadId::new();
        let session = test_thread_session(thread, app.config.cwd.to_path_buf());
        let mut turn = test_turn("background", TurnStatus::InProgress, Vec::new());
        app.enqueue_primary_thread_session(session, vec![turn.clone()]).await?;
        quiet_attention_edit_question(&mut app.chat_widget, thread);
        let questions = app.chat_widget.capture_thread_input_state().unwrap().questions;
        app.enqueue_thread_notification(thread, attention_marker(thread, &turn.id)).await?;
        let marker = app.active_thread_rx.as_mut().unwrap().recv().await.unwrap();
        assert!(matches!(&marker, ThreadBufferedEvent::Notification(n) if matches!(n.as_ref(), ServerNotification::AttentionTurnStarted(_))));
        app.handle_thread_event_now(marker);
        assert!(app.chat_widget.is_background_attention_turn_for_test(&turn.id));
        assert!(snapshot_has_attention_marker(&app.thread_event_channels[&thread].store.lock().await.snapshot(), &turn.id));
        let event = if terminal == 3 {
            ServerNotification::Error(codex_app_server_protocol::ErrorNotification {
                thread_id: thread.to_string(), turn_id: turn.id.clone(), will_retry: false,
                error: codex_app_server_protocol::TurnError { message: "fixture failure".into(), codex_error_info: None, additional_details: None, misalignment: None },
            })
        } else {
            turn.status = [TurnStatus::Completed, TurnStatus::Interrupted, TurnStatus::Failed][terminal].clone();
            ServerNotification::TurnCompleted(codex_app_server_protocol::TurnCompletedNotification { thread_id: thread.to_string(), turn })
        };
        app.enqueue_thread_notification(thread, event).await?;
        let delivered = app.active_thread_rx.as_mut().unwrap().recv().await.unwrap();
        app.handle_thread_event_now(delivered);
        assert_eq!(app.chat_widget.capture_thread_input_state().unwrap().questions, questions);
        assert_eq!(app.chat_widget.composer_text_with_pending(), "main draft");
    }
    Ok(())
}

#[tokio::test]
async fn quiet_attention_full_native_turn_user_clears_widget_and_store_before_terminal() -> Result<()> {
    for completed in [false, true] {
        let (mut app, _events, _ops) = make_test_app_with_channels().await;
        let thread = ThreadId::new();
        let session = test_thread_session(thread, app.config.cwd.to_path_buf());
        app.enqueue_primary_thread_session(session, Vec::new()).await?;
        app.enqueue_thread_notification(thread, attention_marker(thread, "turn")).await?;
        let marker = app.active_thread_rx.as_mut().unwrap().recv().await.unwrap();
        app.handle_thread_event_now(marker);
        assert!(app.chat_widget.is_background_attention_turn_for_test("turn"));
        let turn = test_turn("turn", if completed { TurnStatus::Completed } else { TurnStatus::InProgress }, vec![quiet_attention_native_user()]);
        let event = if completed {
            ServerNotification::TurnCompleted(codex_app_server_protocol::TurnCompletedNotification { thread_id: thread.to_string(), turn })
        } else {
            ServerNotification::TurnStarted(codex_app_server_protocol::TurnStartedNotification { thread_id: thread.to_string(), turn })
        };
        app.enqueue_thread_notification(thread, event).await?;
        let delivered = app.active_thread_rx.as_mut().unwrap().recv().await.unwrap();
        app.handle_thread_event_now(delivered);
        assert!(!app.chat_widget.is_background_attention_turn_for_test("turn"));
        assert!(app.thread_event_channels[&thread].store.lock().await.suppresses_attention_marker(&attention_marker(thread, "turn")));
    }
    Ok(())
}

#[tokio::test]
async fn quiet_attention_old_queued_marker_is_rechecked_after_native_refresh() -> Result<()> {
    let (mut app, _events, _ops) = make_test_app_with_channels().await;
    let thread = ThreadId::new();
    let session = test_thread_session(thread, app.config.cwd.to_path_buf());
    app.enqueue_primary_thread_session(session.clone(), Vec::new()).await?;
    app.enqueue_thread_notification(thread, attention_marker(thread, "turn")).await?;
    let mut snapshot = app.thread_event_channels[&thread].store.lock().await.snapshot();
    assert!(snapshot_has_attention_marker(&snapshot, "turn"));
    app.apply_refreshed_snapshot_thread(thread, AppServerStartedThread {
        session, turns: vec![test_turn("turn", TurnStatus::InProgress, vec![quiet_attention_native_user()])],
        blocks_direct_input: false, task_tools_available: false,
    }, &mut snapshot).await;
    app.replay_thread_snapshot(snapshot, false);
    assert!(!app.chat_widget.is_background_attention_turn_for_test("turn"));
    // The cloned marker was already admitted to the native active channel.
    let old = app.active_thread_rx.as_mut().unwrap().recv().await.unwrap();
    assert!(matches!(&old, ThreadBufferedEvent::Notification(n) if matches!(n.as_ref(), ServerNotification::AttentionTurnStarted(_))));
    app.handle_thread_event_now_recovering_file_changes(old).await;
    assert!(!app.chat_widget.is_background_attention_turn_for_test("turn"));
    Ok(())
}

#[tokio::test]
async fn quiet_attention_captured_snapshot_uses_retained_foreground_after_raw_user_eviction() {
    let (mut app, _events, _ops) = make_test_app_with_channels().await;
    let thread = ThreadId::new();
    let session = test_thread_session(thread, app.config.cwd.to_path_buf());
    let channel = ThreadEventChannel::new_with_session(1, session.clone(), vec![test_turn("turn", TurnStatus::InProgress, Vec::new())]);
    let mut stale = {
        let mut store = channel.store.lock().await;
        store.push_notification(attention_marker(thread, "turn"));
        let stale = store.snapshot();
        store.push_notification(ServerNotification::ItemCompleted(ItemCompletedNotification::new(
            thread.to_string(), "turn".into(), quiet_attention_native_user(), None, 0,
        )));
        store.push_notification(delta(&thread.to_string(), "other", "eviction"));
        assert!(store.suppresses_attention_marker(&attention_marker(thread, "turn")));
        stale
    };
    assert!(snapshot_has_attention_marker(&stale, "turn"));
    channel.store.lock().await.active = true;
    app.thread_event_channels.insert(thread, channel);
    app.apply_refreshed_snapshot_thread(thread, AppServerStartedThread {
        session, turns: vec![test_turn("turn", TurnStatus::InProgress, Vec::new())],
        blocks_direct_input: false, task_tools_available: false,
    }, &mut stale).await;
    assert!(!snapshot_has_attention_marker(&stale, "turn"));
    app.replay_thread_snapshot(stale, false);
    assert!(!app.chat_widget.is_background_attention_turn_for_test("turn"));
}

async fn quiet_attention_polled_voice(
    app: &mut App,
    source: &AppServerSession,
    displayed: crate::backend_id::BackendId,
    notification: ServerNotification,
) {
    app.handle_polled_backend_event(source, displayed, crate::app_server_session::AppServerSessionEvent {
        backend_id: source.backend_id(),
        event: codex_app_server_client::AppServerEvent::ServerNotification(Box::new(notification)),
    }).await;
}

#[tokio::test]
async fn quiet_attention_cross_backend_voice_uses_owned_store_in_both_orders_and_live_terminals() -> Result<()> {
    for marker_first in [false, true] {
        for terminal in 0..4 {
            for foreground in [false, true] {
                let (mut app, _events, _ops) = make_test_app_with_channels().await;
                let thread = ThreadId::new();
                let session = test_thread_session(thread, app.config.cwd.to_path_buf());
                app.chat_widget.handle_thread_session_quiet(session.clone());
                crate::chatwidget::activate_voice_for_thread(&mut app.chat_widget, thread);
                app.chat_widget.park_voice();
                let (visible, _, _, _) = crate::chatwidget::tests::make_chatwidget_manual_with_sender().await;
                app.background_voice = Some(Box::new(std::mem::replace(&mut app.chat_widget, visible)));
                let source = crate::start_embedded_app_server_for_picker(&app.config).await?;
                let displayed = crate::backend_id::BackendId::new();
                let mut channel = ThreadEventChannel::new_with_session(1, session, vec![test_turn("turn", TurnStatus::InProgress, Vec::new())]);
                channel.backend_id = Some(source.backend_id());
                app.thread_event_channels.insert(thread, channel);
                if marker_first {
                    quiet_attention_polled_voice(&mut app, &source, displayed, attention_marker(thread, "turn")).await;
                }
                if foreground {
                    quiet_attention_polled_voice(&mut app, &source, displayed,
                        ServerNotification::ItemCompleted(ItemCompletedNotification::new(
                            thread.to_string(), "turn".into(), quiet_attention_native_user(), None, 0,
                        ))).await;
                    quiet_attention_polled_voice(&mut app, &source, displayed,
                        delta(&thread.to_string(), "other", "raw-user-eviction")).await;
                }
                // Establish pending questions after genuine user handling so
                // terminal classification, not input admission, is the variable.
                quiet_attention_edit_question(app.background_voice.as_mut().unwrap(), thread);
                let before = app.background_voice.as_mut().unwrap().capture_thread_input_state().unwrap().questions;
                quiet_attention_polled_voice(&mut app, &source, displayed, attention_marker(thread, "turn")).await;
                assert_eq!(app.background_voice.as_ref().unwrap().is_background_attention_turn_for_test("turn"), !foreground);
                assert_eq!(snapshot_has_attention_marker(&app.thread_event_channels[&thread].store.lock().await.snapshot(), "turn"), !foreground);
                let event = if terminal == 3 {
                    ServerNotification::Error(codex_app_server_protocol::ErrorNotification {
                        thread_id: thread.to_string(), turn_id: "turn".into(), will_retry: false,
                        error: codex_app_server_protocol::TurnError { message: "fixture failure".into(), codex_error_info: None, additional_details: None, misalignment: None },
                    })
                } else {
                    ServerNotification::TurnCompleted(codex_app_server_protocol::TurnCompletedNotification {
                        thread_id: thread.to_string(), turn: test_turn("turn", [TurnStatus::Completed, TurnStatus::Interrupted, TurnStatus::Failed][terminal].clone(), Vec::new()),
                    })
                };
                quiet_attention_polled_voice(&mut app, &source, displayed, event).await;
                let owner = app.background_voice.as_mut().expect("voice owner retained");
                let after = owner.capture_thread_input_state().unwrap().questions;
                if foreground {
                    assert_ne!(after, before);
                    assert!(owner.composer_text_with_pending().contains("retained answer"));
                } else {
                    assert_eq!(after, before);
                    assert_eq!(owner.composer_text_with_pending(), "main draft");
                }
                assert!(app.active_thread_rx.is_none(), "background source must not create active delivery");
                source.shutdown().await?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn quiet_attention_cross_backend_missing_or_wrong_channel_never_trusts_marker_or_mutates_visible() -> Result<()> {
    for missing in [false, true] {
        let (mut app, _events, _ops) = make_test_app_with_channels().await;
        let thread = ThreadId::new();
        let session = test_thread_session(thread, app.config.cwd.to_path_buf());
        crate::chatwidget::activate_voice_for_thread(&mut app.chat_widget, thread);
        app.chat_widget.park_voice();
        let (visible, _, _, _) = crate::chatwidget::tests::make_chatwidget_manual_with_sender().await;
        app.background_voice = Some(Box::new(std::mem::replace(&mut app.chat_widget, visible)));
        app.chat_widget.handle_thread_session_quiet(session.clone()); // colliding visible native ID
        app.chat_widget.handle_paste("visible draft".into());
        let visible_before = app.chat_widget.capture_thread_input_state();
        let source = crate::start_embedded_app_server_for_picker(&app.config).await?;
        let displayed = crate::backend_id::BackendId::new();
        if !missing {
            let mut channel = ThreadEventChannel::new_with_session(1, session, Vec::new());
            channel.backend_id = Some(displayed);
            app.thread_event_channels.insert(thread, channel);
        }
        quiet_attention_polled_voice(&mut app, &source, displayed, attention_marker(thread, "turn")).await;
        assert!(!app.background_voice.as_ref().unwrap().is_background_attention_turn_for_test("turn"));
        assert_eq!(app.chat_widget.capture_thread_input_state(), visible_before);
        if !missing {
            let store = app.thread_event_channels[&thread].store.lock().await;
            assert!(store.buffer.is_empty());
            assert_eq!(store.active_turn_id(), None);
            assert!(store.pending_replay_requests().is_empty());
        }
        assert!(!app.reconnect.offline);
        // Existing degraded nonmarker lifecycle remains admitted without a channel.
        quiet_attention_polled_voice(&mut app, &source, displayed,
            ServerNotification::ThreadClosed(codex_app_server_protocol::ThreadClosedNotification {
                thread_id: thread.to_string(),
            })).await;
        assert!(app.voice_owner_thread_id().is_none());
        source.shutdown().await?;
    }
    Ok(())
}
