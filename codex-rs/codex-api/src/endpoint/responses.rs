use crate::auth::SharedAuthProvider;
use crate::common::ResponseStream;
use crate::common::ResponsesApiRequest;
use crate::endpoint::log_responses_request;
use crate::endpoint::session::EndpointSession;
use crate::error::ApiError;
use crate::error::PREVIOUS_RESPONSE_NOT_FOUND_CODE;
use crate::provider::Provider;
use crate::requests::Compression;
use crate::requests::headers::build_session_headers;
use crate::requests::headers::insert_header;
use crate::requests::headers::subagent_header;
use crate::sse::spawn_response_stream;
use crate::telemetry::SseTelemetry;
use codex_client::EncodedJsonBody;
use codex_client::HttpTransport;
use codex_client::RequestCompression;
use codex_client::RequestTelemetry;
use codex_client::TransportError;
use codex_protocol::protocol::SessionSource;
use http::HeaderMap;
use http::HeaderValue;
use http::Method;
use serde_json::Value;
use std::sync::Arc;
use std::sync::OnceLock;
use tracing::instrument;

/// Responses-compatible inference routes supported by Codex backend.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ResponsesEndpoint {
    /// Regular user-owned model inference.
    #[default]
    Responses,
    /// Full Guardian approval-review agent inference.
    Guardian,
    /// Lightweight asynchronous Guardian risk classification.
    GuardianClassifier,
}

impl ResponsesEndpoint {
    /// Returns the provider-relative path for this inference surface.
    pub const fn path(self) -> &'static str {
        match self {
            Self::Responses => "/responses",
            Self::Guardian => "/guardian",
            Self::GuardianClassifier => "/guardian-classifier",
        }
    }
}

pub struct ResponsesClient<T: HttpTransport> {
    session: EndpointSession<T>,
    sse_telemetry: Option<Arc<dyn SseTelemetry>>,
    endpoint: ResponsesEndpoint,
}

#[derive(Default)]
pub struct ResponsesOptions {
    pub session_id: Option<String>,
    pub thread_id: Option<String>,
    pub session_source: Option<SessionSource>,
    pub extra_headers: HeaderMap,
    pub compression: Compression,
    pub turn_state: Option<Arc<OnceLock<String>>>,
    /// Runtime-only parent receipt, attached after inference tracing.
    pub guardian_ticket: Option<codex_protocol::guardian_ticket::GuardianTicket>,
}

impl<T: HttpTransport> ResponsesClient<T> {
    pub fn new(transport: T, provider: Provider, auth: SharedAuthProvider) -> Self {
        Self {
            session: EndpointSession::new(transport, provider, auth),
            sse_telemetry: None,
            endpoint: ResponsesEndpoint::Responses,
        }
    }

    /// Selects a Responses-compatible backend route for subsequent requests.
    pub fn with_endpoint(mut self, endpoint: ResponsesEndpoint) -> Self {
        self.endpoint = endpoint;
        self
    }

    pub fn with_telemetry(
        self,
        request: Option<Arc<dyn RequestTelemetry>>,
        sse: Option<Arc<dyn SseTelemetry>>,
    ) -> Self {
        Self {
            session: self.session.with_request_telemetry(request),
            sse_telemetry: sse,
            endpoint: self.endpoint,
        }
    }

    #[instrument(
        name = "responses.stream_request",
        level = "info",
        skip_all,
        fields(
            transport = "responses_http",
            http.method = "POST",
            api.path = self.endpoint.path(),
            request.mode = tracing::field::Empty,
            request.input_items = tracing::field::Empty,
            request.body_bytes = tracing::field::Empty
        )
    )]
    pub async fn stream_request(
        &self,
        mut request: ResponsesApiRequest,
        options: ResponsesOptions,
    ) -> Result<ResponseStream, ApiError> {
        let ResponsesOptions {
            session_id,
            thread_id,
            session_source,
            extra_headers,
            compression,
            turn_state,
            guardian_ticket,
        } = options;
        let request_mode = if request.previous_response_id.is_some() {
            "incremental"
        } else {
            "full"
        };
        let input_items = request.input.len() as u64;
        crate::guardian_ticket::attach(
            &mut request.client_metadata,
            guardian_ticket.as_ref(),
            self.endpoint,
        );

        let mut body = EncodedJsonBody::encode(&request)
            .map_err(|e| ApiError::Stream(format!("failed to encode responses request: {e}")))?;
        if guardian_ticket.is_some() {
            body = body.without_body_logging();
        }
        let span = tracing::Span::current();
        span.record("request.mode", request_mode);
        span.record("request.input_items", input_items);
        span.record("request.body_bytes", body.as_bytes().len() as u64);
        log_responses_request(
            "responses_http",
            request_mode,
            input_items as usize,
            body.as_bytes().len(),
            None,
        );

        let mut headers = extra_headers;
        if let Some(ref thread_id) = thread_id {
            insert_header(&mut headers, "x-client-request-id", thread_id);
        }
        headers.extend(build_session_headers(session_id, thread_id));
        if let Some(subagent) = subagent_header(&session_source) {
            insert_header(&mut headers, "x-openai-subagent", &subagent);
        }

        self.stream_encoded(body, headers, compression, turn_state)
            .await
    }

    #[instrument(
        name = "responses.stream",
        level = "info",
        skip_all,
        fields(
            transport = "responses_http",
            http.method = "POST",
            api.path = self.endpoint.path(),
            turn.has_state = turn_state.is_some()
        )
    )]
    pub async fn stream(
        &self,
        body: Value,
        extra_headers: HeaderMap,
        compression: Compression,
        turn_state: Option<Arc<OnceLock<String>>>,
    ) -> Result<ResponseStream, ApiError> {
        let body = EncodedJsonBody::encode(&body)
            .map_err(|e| ApiError::Stream(format!("failed to encode responses request: {e}")))?;
        self.stream_encoded(body, extra_headers, compression, turn_state)
            .await
    }

    async fn stream_encoded(
        &self,
        body: EncodedJsonBody,
        extra_headers: HeaderMap,
        compression: Compression,
        turn_state: Option<Arc<OnceLock<String>>>,
    ) -> Result<ResponseStream, ApiError> {
        let request_compression = match compression {
            Compression::None => RequestCompression::None,
            Compression::Zstd => RequestCompression::Zstd,
        };

        let stream_response = self
            .session
            .stream_encoded_json_with(
                Method::POST,
                self.endpoint.path(),
                extra_headers,
                Some(body),
                |req| {
                    req.headers.insert(
                        http::header::ACCEPT,
                        HeaderValue::from_static("text/event-stream"),
                    );
                    req.compression = request_compression;
                },
            )
            .await
            .map_err(classify_previous_response_not_found)?;

        Ok(spawn_response_stream(
            stream_response,
            self.session.provider().stream_idle_timeout,
            self.sse_telemetry.clone(),
            turn_state,
        ))
    }
}

fn classify_previous_response_not_found(error: ApiError) -> ApiError {
    let ApiError::Transport(TransportError::Http {
        body: Some(body), ..
    }) = &error
    else {
        return error;
    };
    let is_missing = serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|value| value.get("error")?.get("code")?.as_str().map(str::to_owned))
        .is_some_and(|code| code == PREVIOUS_RESPONSE_NOT_FOUND_CODE);
    if is_missing {
        ApiError::PreviousResponseNotFound
    } else {
        error
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::StatusCode;

    #[test]
    fn classifies_missing_previous_response_http_error() {
        let error = ApiError::Transport(TransportError::Http {
            status: StatusCode::BAD_REQUEST,
            url: None,
            headers: None,
            body: Some(
                serde_json::json!({
                    "error": {
                        "code": "previous_response_not_found",
                        "message": "The referenced response expired."
                    }
                })
                .to_string(),
            ),
        });
        assert!(matches!(
            classify_previous_response_not_found(error),
            ApiError::PreviousResponseNotFound
        ));
    }
}
