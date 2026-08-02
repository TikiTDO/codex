//! Receiver-owned admission and local delivery for typed MCP poke signals.
//!
//! The TUI owns the socket lifecycle so a poke cannot outlive the interactive client it targets.
//! The wire format deliberately carries no prompt text: the closed signal name selects a
//! receiver-owned behavior after the hot-loaded policy admits the attributed source.

use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::mpsc;

use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::ThreadAttentionEvent;
use codex_app_server_protocol::ThreadAttentionHeldReason;
use codex_app_server_protocol::ThreadAttentionKind;
use codex_app_server_protocol::ThreadAttentionParams;
use codex_app_server_protocol::ThreadAttentionResponse;
use codex_protocol::ThreadId;
use serde::Deserialize;
use serde::Serialize;

use super::App;
use crate::app_event::AppEvent;
use crate::app_server_session::AppServerSession;

const POKE_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum PokeSignal {
    Tap,
    Cc,
    Council,
}

impl PokeSignal {
    fn attention_kind(self) -> ThreadAttentionKind {
        match self {
            Self::Tap => ThreadAttentionKind::Periodic,
            Self::Cc => ThreadAttentionKind::DirectedResponse,
            Self::Council => ThreadAttentionKind::Mention,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Tap => "tap",
            Self::Cc => "cc",
            Self::Council => "council",
        }
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct PokeRequest {
    pub(crate) version: u8,
    pub(crate) event_id: String,
    pub(crate) to: String,
    pub(crate) workspace: String,
    pub(crate) member: String,
    pub(crate) session_id: String,
    pub(crate) signal: PokeSignal,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub(crate) enum PokeResponse {
    Started,
    Held { reason: String },
    Rejected { reason: String },
}

pub(crate) type PokeResponseSender = mpsc::Sender<PokeResponse>;

pub(crate) struct PokeListener {
    thread_id: ThreadId,
    #[cfg(unix)]
    _inner: unix::UnixPokeListener,
}

impl PokeListener {
    pub(crate) fn start(
        codex_home: &Path,
        thread_id: ThreadId,
        app_event_tx: crate::app_event_sender::AppEventSender,
    ) -> io::Result<Option<Self>> {
        #[cfg(unix)]
        {
            let Some(inner) = unix::UnixPokeListener::start(codex_home, thread_id, app_event_tx)?
            else {
                return Ok(None);
            };
            Ok(Some(Self {
                thread_id,
                _inner: inner,
            }))
        }
        #[cfg(not(unix))]
        {
            let _ = (codex_home, thread_id, app_event_tx);
            Ok(None)
        }
    }

    pub(crate) fn serves(&self, thread_id: ThreadId) -> bool {
        self.thread_id == thread_id
    }
}

impl App {
    pub(super) fn start_poke_listener(&mut self, thread_id: ThreadId) {
        if self
            .poke_listener
            .as_ref()
            .is_some_and(|listener| listener.serves(thread_id))
        {
            return;
        }
        self.poke_listener = None;
        match PokeListener::start(
            &self.config.codex_home,
            thread_id,
            self.app_event_tx.clone(),
        ) {
            Ok(listener) => self.poke_listener = listener,
            Err(err) => tracing::warn!(%thread_id, %err, "failed to start typed poke listener"),
        }
    }

    pub(super) async fn handle_poke(
        &mut self,
        app_server: &mut AppServerSession,
        request: PokeRequest,
        reply: PokeResponseSender,
    ) {
        let Some(primary_thread_id) = self.primary_thread_id else {
            let _ = reply.send(PokeResponse::Held {
                reason: "noPrimaryThread".to_string(),
            });
            return;
        };
        if request.to != primary_thread_id.to_string() {
            let _ = reply.send(PokeResponse::Rejected {
                reason: "recipientChanged".to_string(),
            });
            return;
        }

        let request_id = app_server.next_request_id();
        let response = app_server
            .request_handle()
            .request_typed::<ThreadAttentionResponse>(ClientRequest::ThreadAttention {
                request_id,
                params: ThreadAttentionParams {
                    thread_id: request.to,
                    attention: ThreadAttentionEvent {
                        version: POKE_VERSION,
                        event_id: request.event_id,
                        kind: request.signal.attention_kind(),
                        source_class: "mcp".to_string(),
                        source_ref: format!(
                            "{}/{}@{}",
                            request.workspace, request.member, request.session_id
                        ),
                        reference: Some(format!("poke/v1/{}", request.signal.as_str())),
                    },
                },
            })
            .await;

        let response = match response {
            Ok(ThreadAttentionResponse::Started {}) => PokeResponse::Started,
            Ok(ThreadAttentionResponse::Held { reason }) => PokeResponse::Held {
                reason: match reason {
                    ThreadAttentionHeldReason::PendingTriggerTurn => "pendingOperatorTurn",
                    ThreadAttentionHeldReason::PlanMode => "planMode",
                    ThreadAttentionHeldReason::Busy => "busy",
                }
                .to_string(),
            },
            Err(err) => {
                tracing::warn!(%err, "typed poke attention outcome is ambiguous");
                PokeResponse::Held {
                    reason: "attentionOutcomeUnknown".to_string(),
                }
            }
        };
        let _ = reply.send(response);
    }
}

#[cfg(unix)]
#[path = "poke_unix.rs"]
mod unix;
