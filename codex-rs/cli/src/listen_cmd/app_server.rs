use anyhow::Context;
use anyhow::Result;
use codex_app_server_client::AppServerClient;
use codex_app_server_client::AppServerEvent;
use codex_app_server_client::AppServerRequestHandle;
use codex_app_server_client::RemoteAppServerClient;
use codex_app_server_client::RemoteAppServerConnectArgs;
use codex_app_server_client::RemoteAppServerEndpoint;
use codex_app_server_client::TypedRequestError;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadAttentionEvent as RpcAttentionEvent;
use codex_app_server_protocol::ThreadAttentionParams;
use codex_app_server_protocol::ThreadAttentionResponse;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_utils_absolute_path::AbsolutePathBuf;
use tokio::sync::oneshot;

pub(super) struct AppServerRequests {
    handle: AppServerRequestHandle,
    next_request_id: i64,
}

impl AppServerRequests {
    fn new(handle: AppServerRequestHandle) -> Self {
        Self {
            handle,
            next_request_id: 1,
        }
    }

    fn next_request_id(&mut self) -> RequestId {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.saturating_add(1);
        RequestId::Integer(request_id)
    }

    pub(super) async fn attention(
        &mut self,
        thread_id: &str,
        attention: RpcAttentionEvent,
    ) -> std::result::Result<ThreadAttentionResponse, TypedRequestError> {
        let request_id = self.next_request_id();
        self.handle
            .request_typed(ClientRequest::ThreadAttention {
                request_id,
                params: ThreadAttentionParams {
                    thread_id: thread_id.to_string(),
                    attention,
                },
            })
            .await
    }

    /// Resume the exact thread and keep this connection subscribed to it.
    ///
    /// `thread/attention` only accepts loaded threads. A loaded thread with no
    /// subscribers is intentionally unloaded after an idle grace period, so a
    /// durable listener must itself hold a subscription rather than spend
    /// periodic model turns merely to keep the thread resident.
    pub(super) async fn subscribe(
        &mut self,
        thread_id: &str,
    ) -> std::result::Result<ThreadResumeResponse, TypedRequestError> {
        let request_id = self.next_request_id();
        self.handle
            .request_typed(ClientRequest::ThreadResume {
                request_id,
                params: subscription_resume_params(thread_id),
            })
            .await
    }
}

fn subscription_resume_params(thread_id: &str) -> ThreadResumeParams {
    ThreadResumeParams {
        thread_id: thread_id.to_string(),
        // The listener needs only the live subscription. Avoid returning the
        // full transcript or overriding any persisted thread settings.
        exclude_turns: true,
        ..ThreadResumeParams::default()
    }
}

pub(super) async fn connect(
    socket_path: AbsolutePathBuf,
) -> Result<(AppServerRequests, oneshot::Receiver<String>)> {
    let client = RemoteAppServerClient::connect(RemoteAppServerConnectArgs {
        endpoint: RemoteAppServerEndpoint::UnixSocket { socket_path },
        client_name: "codex-listen".to_string(),
        client_version: env!("CARGO_PKG_VERSION").to_string(),
        experimental_api: true,
        suppress_automatic_thread_subscription: true,
        mcp_server_openai_form_elicitation: false,
        opt_out_notification_methods: Vec::new(),
        channel_capacity: 128,
    })
    .await
    .context("failed to connect listener to app-server daemon")?;
    let client = AppServerClient::Remote(client);
    let requests = AppServerRequests::new(client.request_handle());
    let (disconnect_tx, disconnect_rx) = oneshot::channel();
    tokio::spawn(async move {
        let disconnect_reason = drain_app_server_events(client).await;
        let _ = disconnect_tx.send(disconnect_reason);
    });
    Ok((requests, disconnect_rx))
}

async fn drain_app_server_events(mut client: AppServerClient) -> String {
    while let Some(event) = client.next_event().await {
        match event {
            AppServerEvent::Disconnected { message } => return message,
            AppServerEvent::ServerRequest(request) => {
                eprintln!(
                    "codex listen: ignored global app-server request {} so another client can respond",
                    request.id()
                );
            }
            AppServerEvent::Lagged { skipped } => {
                eprintln!("codex listen: app-server event stream skipped {skipped} events");
            }
            AppServerEvent::ServerNotification(_) => {}
        }
    }
    "app-server event stream closed".to_string()
}

#[cfg(test)]
mod tests {
    use super::subscription_resume_params;

    #[test]
    fn subscription_resume_is_metadata_only_and_does_not_override_thread_settings() {
        let params = subscription_resume_params("019faa64-c0a7-7193-873b-f71cf30951d7");

        assert_eq!(params.thread_id, "019faa64-c0a7-7193-873b-f71cf30951d7");
        assert!(params.exclude_turns);
        assert!(params.history.is_none());
        assert!(params.path.is_none());
        assert!(params.model.is_none());
        assert!(params.model_provider.is_none());
        assert!(params.cwd.is_none());
        assert!(params.approval_policy.is_none());
        assert!(params.sandbox.is_none());
        assert!(params.permissions.is_none());
        assert!(params.config.is_none());
        assert!(params.base_instructions.is_none());
        assert!(params.developer_instructions.is_none());
    }
}
