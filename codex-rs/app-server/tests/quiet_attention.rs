//! Disposable native API fixtures. The local HTTP server replays canned
//! Responses events; no live model, credential, operator term or adapter runs.

use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use app_test_support::create_final_assistant_message_sse_response;
use app_test_support::create_mock_responses_server_sequence;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::JSONRPCMessage;
use codex_app_server_protocol::ThreadAttentionDelivery;
use codex_app_server_protocol::ThreadAttentionListParams;
use codex_app_server_protocol::ThreadAttentionListResponse;
use codex_app_server_protocol::ThreadAttentionOrigin;
use codex_app_server_protocol::ThreadAttentionOriginKind;
use codex_app_server_protocol::ThreadAttentionParams;
use codex_app_server_protocol::ThreadAttentionResponse;
use codex_app_server_protocol::ThreadItemPayload;
use codex_app_server_protocol::ThreadHistoryMode;
use codex_app_server_protocol::AttentionTurnStartedNotification;
use codex_app_server_protocol::TurnStartedNotification;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadStartParams;
use serde_json::Value;
use tempfile::TempDir;
use tokio::time::Duration;
use tokio::time::timeout;

const READ_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
async fn quiet_attention_idle_is_internal_ordered_and_deduplicated_after_cold_resume() -> Result<()> {
    for mode in [None, Some(ThreadHistoryMode::Legacy)] {
        for exclude_turns in [false, true] {
            quiet_attention_idle_case(mode, exclude_turns).await?;
        }
    }
    Ok(())
}

async fn quiet_attention_idle_case(mode: Option<ThreadHistoryMode>, exclude_turns: bool) -> Result<()> {
    let server =
        create_mock_responses_server_sequence(vec![create_final_assistant_message_sse_response(
            "fixture attention complete",
        )?])
        .await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(home.path())?;
    let mut app = TestAppServer::builder()
        .with_codex_home(home.path())
        .without_managed_config()
        .build_initialized()
        .await?;
    let thread = app.start_thread(ThreadStartParams { history_mode: mode, ..Default::default() }).await?.thread;
    assert_eq!(thread.history_mode, mode.unwrap_or(ThreadHistoryMode::Paginated));
    let params = ThreadAttentionParams {
        thread_id: thread.id.clone(),
        batch_id: "quiet-fixture:idle".into(),
        origins: vec![ThreadAttentionOrigin {
            kind: ThreadAttentionOriginKind::Peer,
            source_ref: "cue:fixture".into(),
            sender: Some("fixture-peer".into()),
        }],
    };
    let receipt: ThreadAttentionResponse = timeout(
        READ_TIMEOUT,
        app.request(|request_id| ClientRequest::ThreadAttention {
            request_id,
            params: params.clone(),
        }),
    )
    .await??;
    let ThreadAttentionResponse::Accepted {
        delivery: ThreadAttentionDelivery::Started,
        turn_id: Some(turn_id),
    } = &receipt
    else {
        panic!("idle attention must start internally: {receipt:?}");
    };

    let mut marker_seen = false;
    let mut start_seen = false;
    timeout(READ_TIMEOUT, async {
        loop {
            let JSONRPCMessage::Notification(notification) = app.read_next_message().await? else {
                continue;
            };
            let p = notification.params.unwrap_or(Value::Null);
            if p.get("threadId").and_then(Value::as_str) != Some(thread.id.as_str()) {
                continue;
            }
            match notification.method.as_str() {
                "attention/turn/started" => {
                    assert!(!start_seen, "native provenance arrived after TurnStarted");
                    assert_eq!(p["turnId"], *turn_id);
                    marker_seen = true;
                }
                "turn/started" => {
                    assert!(marker_seen, "idle task started before native provenance");
                    assert_eq!(p["turn"]["id"], *turn_id);
                    start_seen = true;
                }
                "item/started" | "item/completed" => {
                    assert_ne!(
                        p["item"]["type"], "userMessage",
                        "quiet attention emitted user input"
                    );
                }
                "turn/completed" => {
                    assert!(marker_seen && start_seen);
                    assert_eq!(p["turn"]["id"], *turn_id);
                    break;
                }
                _ => {}
            }
        }
        Ok::<(), anyhow::Error>(())
    })
    .await??;
    app.clear_message_buffer();
    let warm = timeout(READ_TIMEOUT, quiet_attention_resume_barrier(&mut app, &thread.id, exclude_turns)).await??;
    assert_eq!(warm.thread.history_mode, thread.history_mode);
    assert_eq!(warm.thread.turns.is_empty(), exclude_turns);
    let restored: AttentionTurnStartedNotification = timeout(READ_TIMEOUT,
        app.read_notification("attention/turn/started")).await??;
    assert_eq!(restored.thread_id, thread.id);
    assert_eq!(&restored.turn_id, turn_id);
    app.clear_message_buffer();
    let requests = server.received_requests().await.unwrap();
    let requests = requests
        .iter()
        .filter(|request| request.url.path().ends_with("/responses"))
        .collect::<Vec<_>>();
    assert_eq!(requests.len(), 1);
    let input = requests[0].body_json::<Value>()?["input"].to_string();
    assert!(input.contains("codex_internal_context"));
    assert!(input.contains("cue:fixture"));
    // Cold recovery requires the old server's actual persistence teardown, not
    // merely a dropped observer or kill-on-drop's best-effort process cleanup.
    assert!(
        timeout(READ_TIMEOUT, app.shutdown_gracefully())
            .await??
            .success()
    );
    drop(app);

    let mut app = TestAppServer::builder()
        .with_codex_home(home.path())
        .without_managed_config()
        .build_initialized()
        .await?;
    let resumed: ThreadResumeResponse = timeout(
        READ_TIMEOUT,
        app.request(|request_id| ClientRequest::ThreadResume {
            request_id,
            params: ThreadResumeParams {
                thread_id: thread.id.clone(),
                exclude_turns,
                ..Default::default()
            },
        }),
    )
    .await??;
    assert!(
        resumed
            .thread
            .turns
            .iter()
            .flat_map(|turn| &turn.items)
            .all(|item| !matches!(&item.payload, ThreadItemPayload::UserMessage { .. }))
    );
    let retry: ThreadAttentionResponse = timeout(
        READ_TIMEOUT,
        app.request(|request_id| ClientRequest::ThreadAttention {
            request_id,
            params: params.clone(),
        }),
    )
    .await??;
    assert_eq!(retry, receipt);
    let snapshot: ThreadAttentionListResponse = timeout(
        READ_TIMEOUT,
        app.request(|request_id| ClientRequest::ThreadAttentionList {
            request_id,
            params: ThreadAttentionListParams {
                thread_id: thread.id.clone(),
            },
        }),
    )
    .await??;
    assert_eq!(snapshot.notices.len(), 1);
    assert_eq!(snapshot.notices[0].batch_id, params.batch_id);
    assert_eq!(snapshot.notices[0].origins, params.origins);
    assert_eq!(snapshot.notices[0].admission, receipt);
    assert_eq!(resumed.thread.history_mode, thread.history_mode);
    assert_eq!(resumed.thread.turns.is_empty(), exclude_turns);
    // Retry/list share the exact-thread serialization queue with resume and
    // occur after its listener restoration, so this is a real cold-replay marker.
    let restored: AttentionTurnStartedNotification = timeout(READ_TIMEOUT,
        app.read_notification("attention/turn/started")).await??;
    assert_eq!(restored.thread_id, thread.id);
    assert_eq!(&restored.turn_id, turn_id);
    assert_eq!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.url.path().ends_with("/responses"))
            .count(),
        1,
        "cold retry started another model request"
    );
    assert!(
        timeout(READ_TIMEOUT, app.shutdown_gracefully())
            .await??
            .success()
    );
    Ok(())
}

#[tokio::test]
async fn quiet_attention_invalid_metadata_never_admits_or_calls_model() -> Result<()> {
    let server = create_mock_responses_server_sequence(Vec::new()).await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(home.path())?;
    let mut app = TestAppServer::builder()
        .with_codex_home(home.path())
        .without_managed_config()
        .build_initialized()
        .await?;
    let thread = app.start_thread(ThreadStartParams::default()).await?.thread;
    let params = ThreadAttentionParams {
        thread_id: thread.id.clone(),
        batch_id: "quiet-fixture:invalid".into(),
        origins: vec![ThreadAttentionOrigin {
            kind: ThreadAttentionOriginKind::Peer,
            source_ref: "this is prose, not a coordinate".into(),
            sender: Some("fixture-peer".into()),
        }],
    };
    let request_id = app
        .send_request("thread/attention", Some(serde_json::to_value(params)?))
        .await?;
    let rejected = timeout(
        READ_TIMEOUT,
        app.read_stream_until_error_message(codex_app_server_protocol::RequestId::Integer(
            request_id,
        )),
    )
    .await??;
    assert_eq!(rejected.error.code, -32600);
    assert!(rejected.error.message.contains("bounded references"));
    let snapshot: ThreadAttentionListResponse = app
        .request(|request_id| ClientRequest::ThreadAttentionList {
            request_id,
            params: ThreadAttentionListParams {
                thread_id: thread.id,
            },
        })
        .await?;
    assert!(snapshot.notices.is_empty());
    assert!(server.received_requests().await.unwrap().is_empty());
    assert!(
        timeout(READ_TIMEOUT, app.shutdown_gracefully())
            .await??
            .success()
    );
    Ok(())
}

// The next exact-thread RPC is serialized behind the entire resume listener
// command (including restoration), not merely behind its early response. Both
// marker notifications and the barrier reply share the outgoing FIFO. Reading
// through that reply therefore makes buffered absence meaningful.
async fn quiet_attention_resume_barrier(
    app: &mut TestAppServer,
    thread_id: &str,
    exclude_turns: bool,
) -> Result<ThreadResumeResponse> {
    let resumed = app.request(|request_id| ClientRequest::ThreadResume {
        request_id,
        params: ThreadResumeParams { thread_id: thread_id.into(), exclude_turns, ..Default::default() },
    }).await?;
    let _: ThreadAttentionListResponse = app.request(|request_id| ClientRequest::ThreadAttentionList {
        request_id,
        params: ThreadAttentionListParams { thread_id: thread_id.into() },
    }).await?;
    Ok(resumed)
}

#[tokio::test]
async fn quiet_attention_consumed_genuine_steer_warm_resume_does_not_restore_background() -> Result<()> {
    for mode in [None, Some(ThreadHistoryMode::Legacy)] {
        for exclude_turns in [false, true] {
            quiet_attention_consumed_steer_case(mode, exclude_turns).await?;
        }
    }
    Ok(())
}

async fn quiet_attention_consumed_steer_case(mode: Option<ThreadHistoryMode>, exclude_turns: bool) -> Result<()> {
    use codex_app_server_protocol::{TurnSteerParams, TurnSteerResponse, UserInput};
    use core_test_support::responses::{sse, ev_response_created, ev_completed, ev_assistant_message};
    use core_test_support::streaming_sse::{start_streaming_sse_server, StreamingSseChunk};
    use tokio::sync::oneshot;

    let (release_first, first_gate) = oneshot::channel();
    let (release_second, second_gate) = oneshot::channel();
    let (server, _) = start_streaming_sse_server(vec![
        vec![StreamingSseChunk { gate: None, body: sse(vec![ev_response_created("quiet-first")]) },
            StreamingSseChunk { gate: Some(first_gate), body: sse(vec![ev_completed("quiet-first")]) }],
        vec![StreamingSseChunk { gate: None, body: sse(vec![ev_response_created("quiet-second")]) },
            StreamingSseChunk { gate: Some(second_gate), body: sse(vec![
                ev_assistant_message("quiet-final", "fixture complete"), ev_completed("quiet-second"),
            ]) }],
    ]).await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(server.uri()).write(home.path())?;
    let mut app = TestAppServer::builder().with_codex_home(home.path())
        .without_managed_config().build_initialized().await?;
    let thread = app.start_thread(ThreadStartParams { history_mode: mode, ..Default::default() }).await?.thread;
    assert_eq!(thread.history_mode, mode.unwrap_or(ThreadHistoryMode::Paginated));
    let receipt: ThreadAttentionResponse = app.request(|request_id| ClientRequest::ThreadAttention {
        request_id, params: ThreadAttentionParams {
            thread_id: thread.id.clone(), batch_id: "quiet-fixture:consumed-steer".into(),
            origins: vec![ThreadAttentionOrigin { kind: ThreadAttentionOriginKind::Peer,
                source_ref: "cue:consumed-fixture".into(), sender: Some("fixture-peer".into()) }],
        },
    }).await?;
    let ThreadAttentionResponse::Accepted { delivery: ThreadAttentionDelivery::Started,
        turn_id: Some(turn_id) } = receipt else { panic!("idle attention was not started: {receipt:?}") };
    timeout(READ_TIMEOUT, server.wait_for_request_count(1)).await?;
    // Read BOTH original events before clearing; unread stdout is not the buffer.
    let original: AttentionTurnStartedNotification = timeout(READ_TIMEOUT,
        app.read_notification("attention/turn/started")).await??;
    assert_eq!(original.thread_id, thread.id);
    assert_eq!(original.turn_id, turn_id);
    let original_start: TurnStartedNotification = timeout(READ_TIMEOUT,
        app.read_notification("turn/started")).await??;
    assert_eq!(original_start.thread_id, thread.id);
    assert_eq!(original_start.turn.id, turn_id);
    app.clear_message_buffer();
    // Genuine background warm-resume positive through the very same route.
    let background = timeout(READ_TIMEOUT, quiet_attention_resume_barrier(&mut app, &thread.id, exclude_turns)).await??;
    assert!(background.thread.turns.iter().flat_map(|turn| &turn.items)
        .all(|item| !matches!(item.payload, ThreadItemPayload::UserMessage { .. })));
    assert_eq!(background.thread.turns.is_empty(), exclude_turns);
    let restored: AttentionTurnStartedNotification = timeout(READ_TIMEOUT,
        app.read_notification("attention/turn/started")).await??;
    assert_eq!(restored.thread_id, thread.id);
    assert_eq!(restored.turn_id, turn_id);
    app.clear_message_buffer();

    let text = "GENUINE_STEER_MUST_BE_CONSUMED_UNDER_SAME_ID";
    let steered: TurnSteerResponse = app.request(|request_id| ClientRequest::TurnSteer {
        request_id, params: TurnSteerParams {
            thread_id: thread.id.clone(), client_user_message_id: Some("fixture-genuine-user".into()),
            input: vec![UserInput::Text { text: text.into(), text_elements: Vec::new() }],
            responsesapi_client_metadata: None, responsesapi_client_metadata_patch: None,
            additional_context: None, expected_turn_id: turn_id.clone(),
        },
    }).await?;
    assert_eq!(steered.turn_id, turn_id);
    release_first.send(()).expect("first native response gate");
    // A steer response is only pending admission. The second native model
    // request must independently contain this input before we call it consumed.
    timeout(READ_TIMEOUT, server.wait_for_request_count(2)).await?;
    let requests = server.requests().await;
    let consumed: Value = serde_json::from_slice(&requests[1])?;
    assert!(consumed["input"].to_string().contains(text));
    timeout(READ_TIMEOUT, async {
        loop {
            let JSONRPCMessage::Notification(n) = app.read_next_message().await? else { continue };
            let p = n.params.unwrap_or(Value::Null);
            if n.method == "item/completed" && p["threadId"] == thread.id
                && p["turnId"] == turn_id && p["item"]["type"] == "userMessage" {
                break;
            }
        }
        Ok::<(), anyhow::Error>(())
    }).await??;
    app.clear_message_buffer();
    let resumed = timeout(READ_TIMEOUT, quiet_attention_resume_barrier(&mut app, &thread.id, exclude_turns)).await??;
    assert_eq!(resumed.thread.turns.is_empty(), exclude_turns);
    if !exclude_turns {
        let resumed_turn = resumed.thread.turns.iter().find(|turn| turn.id == turn_id)
            .expect("same active turn must resume");
        assert!(resumed_turn.items.iter().any(|item| matches!(item.payload, ThreadItemPayload::UserMessage { .. })));
    }
    assert!(!app.pending_notification_methods().iter().any(|m| m == "attention/turn/started"),
        "historical background provenance was republished after genuine input");
    release_second.send(()).expect("second native response gate");
    timeout(READ_TIMEOUT, async {
        loop {
            let JSONRPCMessage::Notification(n) = app.read_next_message().await? else { continue };
            let p = n.params.unwrap_or(Value::Null);
            if n.method == "turn/completed" && p["threadId"] == thread.id && p["turn"]["id"] == turn_id { break; }
        }
        Ok::<(), anyhow::Error>(())
    }).await??;
    assert!(timeout(READ_TIMEOUT, app.shutdown_gracefully()).await??.success());
    drop(app);
    let mut app = TestAppServer::builder().with_codex_home(home.path())
        .without_managed_config().build_initialized().await?;
    let cold = timeout(READ_TIMEOUT, quiet_attention_resume_barrier(&mut app, &thread.id, exclude_turns)).await??;
    assert_eq!(cold.thread.history_mode, thread.history_mode);
    assert_eq!(cold.thread.turns.is_empty(), exclude_turns);
    if !exclude_turns {
        assert!(cold.thread.turns.iter().find(|turn| turn.id == turn_id)
            .expect("cold retained genuine turn").items.iter()
            .any(|item| matches!(item.payload, ThreadItemPayload::UserMessage { .. })));
    }
    assert!(!app.pending_notification_methods().iter().any(|m| m == "attention/turn/started"),
        "cold resume restored background after independently consumed genuine input");
    assert_eq!(server.requests().await.len(), 2, "resume must not request another model response");
    assert!(timeout(READ_TIMEOUT, app.shutdown_gracefully()).await??.success());
    server.shutdown().await;
    Ok(())
}
