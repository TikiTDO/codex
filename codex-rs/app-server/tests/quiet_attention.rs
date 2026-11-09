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
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadStartParams;
use serde_json::Value;
use tempfile::TempDir;
use tokio::time::Duration;
use tokio::time::timeout;

const READ_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
async fn quiet_attention_idle_is_internal_ordered_and_deduplicated_after_cold_resume() -> Result<()>
{
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
    let thread = app.start_thread(ThreadStartParams::default()).await?.thread;
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
