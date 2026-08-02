use std::time::Duration;

use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::ThreadAttentionEvent;
use codex_app_server_protocol::ThreadAttentionHeldReason;
use codex_app_server_protocol::ThreadAttentionKind;
use codex_app_server_protocol::ThreadAttentionParams;
use codex_app_server_protocol::ThreadAttentionResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::UserInput;
use core_test_support::responses;
use serde_json::Value;
use tempfile::TempDir;
use tokio::time::timeout;

const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(10);
const STARTED_MARKER: &str = "<codex_internal_context source=\"attention\">\n<codex-attention version=\"1\" event=\"evt-start\" kind=\"mention\" source-class=\"chat\" source-ref=\"message/42\" reference=\"chat/message/42\" />\n</codex_internal_context>";

fn attention(event_id: &str) -> ThreadAttentionEvent {
    ThreadAttentionEvent {
        version: 1,
        event_id: event_id.to_string(),
        kind: ThreadAttentionKind::Mention,
        source_class: "chat".to_string(),
        source_ref: "message/42".to_string(),
        reference: Some("chat/message/42".to_string()),
    }
}

fn input_contains_text(input: &[Value], expected: &str) -> bool {
    input.iter().any(|item| {
        item.get("content")
            .and_then(Value::as_array)
            .is_some_and(|content| {
                content
                    .iter()
                    .any(|part| part.get("text").and_then(Value::as_str) == Some(expected))
            })
    })
}

#[tokio::test]
async fn thread_attention_starts_idle_turn_with_server_generated_marker() -> Result<()> {
    let server = responses::start_mock_server().await;
    let response_mock = responses::mount_sse_once(
        &server,
        responses::sse(vec![
            responses::ev_response_created("resp-1"),
            responses::ev_assistant_message("msg-1", "Done"),
            responses::ev_completed("resp-1"),
        ]),
    )
    .await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;
    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let ThreadStartResponse { thread, .. } = mcp
        .start_thread(ThreadStartParams {
            model: Some("mock-model".to_string()),
            ..Default::default()
        })
        .await?;

    let response: ThreadAttentionResponse = mcp
        .request(|request_id| ClientRequest::ThreadAttention {
            request_id,
            params: ThreadAttentionParams {
                thread_id: thread.id,
                attention: attention("evt-start"),
            },
        })
        .await?;
    assert_eq!(response, ThreadAttentionResponse::Started {});
    timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_notification_message("turn/completed"),
    )
    .await??;

    let input = response_mock.single_request().input();
    assert!(
        input_contains_text(&input, STARTED_MARKER),
        "model input must contain only the server-rendered attention marker for this event"
    );
    Ok(())
}

#[tokio::test]
async fn thread_attention_holds_busy_without_queuing_or_injecting() -> Result<()> {
    let server = responses::start_mock_server().await;
    let delayed_response = responses::sse_response(responses::sse(vec![
        responses::ev_response_created("resp-1"),
        responses::ev_assistant_message("msg-1", "Done"),
        responses::ev_completed("resp-1"),
    ]))
    .set_delay(Duration::from_secs(2));
    let response_mock = responses::mount_response_once(&server, delayed_response).await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;
    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let ThreadStartResponse { thread, .. } = mcp
        .start_thread(ThreadStartParams {
            model: Some("mock-model".to_string()),
            ..Default::default()
        })
        .await?;
    let _: TurnStartResponse = mcp
        .request(|request_id| ClientRequest::TurnStart {
            request_id,
            params: TurnStartParams {
                thread_id: thread.id.clone(),
                client_user_message_id: None,
                input: vec![UserInput::Text {
                    text: "keep this turn active".to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        })
        .await?;
    timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_notification_message("turn/started"),
    )
    .await??;

    let response: ThreadAttentionResponse = mcp
        .request(|request_id| ClientRequest::ThreadAttention {
            request_id,
            params: ThreadAttentionParams {
                thread_id: thread.id,
                attention: attention("evt-held"),
            },
        })
        .await?;
    assert_eq!(
        response,
        ThreadAttentionResponse::Held {
            reason: ThreadAttentionHeldReason::Busy,
        }
    );
    timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_notification_message("turn/completed"),
    )
    .await??;

    let requests = response_mock.requests();
    assert_eq!(
        requests.len(),
        1,
        "held attention must not queue another turn"
    );
    assert!(
        !input_contains_text(
            &requests[0].input(),
            "<codex_internal_context source=\"attention\">\n<codex-attention version=\"1\" event=\"evt-held\" kind=\"mention\" source-class=\"chat\" source-ref=\"message/42\" reference=\"chat/message/42\" />\n</codex_internal_context>",
        ),
        "held attention must not inject into the active turn"
    );
    Ok(())
}
