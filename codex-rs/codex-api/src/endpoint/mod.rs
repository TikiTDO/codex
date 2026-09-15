pub(crate) mod compact;
pub(crate) mod images;
pub(crate) mod memories;
pub(crate) mod models;
pub(crate) mod realtime_call;
pub(crate) mod realtime_websocket;
pub(crate) mod responses;
pub(crate) mod responses_websocket;
pub(crate) mod search;
mod session;

pub use compact::CompactClient;

const LARGE_RESPONSES_REQUEST_BYTES: usize = 8 * 1024 * 1024;

fn log_responses_request(
    transport: &'static str,
    request_mode: &'static str,
    input_items: usize,
    body_bytes: usize,
    connection_reused: Option<bool>,
) {
    tracing::info!(
        target: "codex_api::responses_request",
        transport,
        request.mode = request_mode,
        request.input_items = input_items,
        request.body_bytes = body_bytes,
        transport.connection_reused = ?connection_reused,
        "Responses request prepared"
    );

    if body_bytes >= LARGE_RESPONSES_REQUEST_BYTES {
        tracing::warn!(
            target: "codex_api::responses_request",
            transport,
            request.mode = request_mode,
            request.input_items = input_items,
            request.body_bytes = body_bytes,
            transport.connection_reused = ?connection_reused,
            "large Responses request prepared"
        );
    }
}

pub use images::ImagesClient;
pub use memories::MemoriesClient;
pub use models::ModelsClient;
pub use realtime_call::RealtimeCallClient;
pub use realtime_call::RealtimeCallResponse;
pub use realtime_websocket::RealtimeContextAppendChannel;
pub use realtime_websocket::RealtimeEventParser;
pub use realtime_websocket::RealtimeOutputModality;
pub use realtime_websocket::RealtimeSessionConfig;
pub use realtime_websocket::RealtimeSessionMode;
pub use realtime_websocket::RealtimeTranscriptState;
pub use realtime_websocket::RealtimeWebsocketClient;
pub use realtime_websocket::RealtimeWebsocketConnection;
pub use realtime_websocket::RealtimeWebsocketEvents;
pub use realtime_websocket::RealtimeWebsocketWriter;
pub use realtime_websocket::session_update_session_json;
pub use responses::ResponsesClient;
pub use responses::ResponsesEndpoint;
pub use responses::ResponsesOptions;
pub use responses_websocket::ResponsesWebsocketClient;
pub use responses_websocket::ResponsesWebsocketClose;
pub use responses_websocket::ResponsesWebsocketConnection;
pub use responses_websocket::ResponsesWebsocketProbe;
pub use search::SearchClient;
