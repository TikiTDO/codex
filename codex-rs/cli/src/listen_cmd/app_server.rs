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
