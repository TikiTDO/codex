//! Lifecycle-owned bridge from the durable Observatory workspace inbox to typed attention.
//!
//! The external bridge owns database credentials, membership, polling, and acknowledgement. The
//! TUI receives only closed, payload-free event metadata and reports whether attention started.

use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::ThreadAttentionEvent;
use codex_app_server_protocol::ThreadAttentionHeldReason;
use codex_app_server_protocol::ThreadAttentionKind;
use codex_app_server_protocol::ThreadAttentionParams;
use codex_app_server_protocol::ThreadAttentionResponse;
use codex_protocol::ThreadId;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use super::App;
use crate::app_event::AppEvent;
use crate::app_server_session::AppServerSession;

const BRIDGE_ENVIRONMENT: &str = "OBSERVATORY_WORKSPACE_SIGNAL_BRIDGE";
const BRIDGE_PROTOCOL: &str = "observatory.workspace_signal_bridge.v1";
const ATTENTION_VERSION: u8 = 1;
const MAX_FRAME_BYTES: usize = 16 * 1024;
const MAX_TARGETS: usize = 16;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) enum WorkspaceSignal {
    Tap,
    Cc,
    Council,
}

impl WorkspaceSignal {
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

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct WorkspaceSignalEvent {
    pub(crate) created_at: String,
    pub(crate) delivery_mode: String,
    pub(crate) event_id: String,
    pub(crate) event_sequence: String,
    pub(crate) from: String,
    pub(crate) predates_runtime: bool,
    pub(crate) priority: u8,
    pub(crate) signal: WorkspaceSignal,
    pub(crate) targets: Vec<String>,
    pub(crate) to: String,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct WorkspaceSignalRequest {
    pub(crate) event: WorkspaceSignalEvent,
    pub(crate) workspace: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkspaceSignalOutcome {
    Started,
    Held,
}

pub(crate) type WorkspaceSignalResponseSender = mpsc::Sender<WorkspaceSignalOutcome>;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Resolution<'a> {
    r#type: &'static str,
    event_id: &'a str,
    outcome: &'static str,
}

pub(crate) struct WorkspaceSignalBridge {
    thread_id: ThreadId,
    child: Arc<Mutex<Option<Child>>>,
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl WorkspaceSignalBridge {
    pub(crate) fn start(
        thread_id: ThreadId,
        app_event_tx: crate::app_event_sender::AppEventSender,
    ) -> io::Result<Option<Self>> {
        let Some(program) = std::env::var_os(BRIDGE_ENVIRONMENT).map(PathBuf::from) else {
            return Ok(None);
        };
        if !program.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{BRIDGE_ENVIRONMENT} must be an absolute path"),
            ));
        }

        let runtime_session_id = thread_id.to_string();
        let mut child = Command::new(program)
            .args(["stream", "--session-id", &runtime_session_id])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("workspace bridge stdin is unavailable"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("workspace bridge stdout is unavailable"))?;
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let child = Arc::new(Mutex::new(Some(child)));
        let worker_child = Arc::clone(&child);
        let worker = match thread::Builder::new()
            .name(format!("workspace-signal-{}", &thread_id.to_string()[..8]))
            .spawn(move || {
                run_bridge(
                    stdout,
                    stdin,
                    app_event_tx,
                    &runtime_session_id,
                    &worker_stop,
                );
                terminate_child(&worker_child);
            }) {
            Ok(worker) => worker,
            Err(err) => {
                terminate_child(&child);
                return Err(err);
            }
        };
        Ok(Some(Self {
            thread_id,
            child,
            stop,
            worker: Some(worker),
        }))
    }

    pub(crate) fn serves(&self, thread_id: ThreadId) -> bool {
        self.thread_id == thread_id
    }
}

impl Drop for WorkspaceSignalBridge {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        // Killing closes stdout and wakes the reader; joining before this can block forever.
        terminate_child(&self.child);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn terminate_child(child: &Mutex<Option<Child>>) {
    let Ok(mut child) = child.lock() else {
        tracing::warn!("workspace signal bridge child lock was poisoned");
        return;
    };
    let Some(mut child) = child.take() else {
        return;
    };
    let _ = child.kill();
    let _ = child.wait();
}

fn run_bridge(
    stdout: impl io::Read,
    mut stdin: impl Write,
    app_event_tx: crate::app_event_sender::AppEventSender,
    expected_runtime_session_id: &str,
    stop: &AtomicBool,
) {
    let mut member = None;
    let mut workspace = None;
    for line in BufReader::new(stdout).lines() {
        let line = match line {
            Ok(line) if line.len() <= MAX_FRAME_BYTES => line,
            Ok(_) => {
                tracing::warn!("workspace signal bridge frame exceeded the bounded limit");
                break;
            }
            Err(err) => {
                tracing::warn!(%err, "workspace signal bridge output failed");
                break;
            }
        };
        match parse_frame(&line, member.as_deref(), workspace.as_deref()) {
            Ok(BridgeFrame::Ready {
                asserted_session_id,
                member: ready_member,
                workspace: ready_workspace,
            }) => {
                if member.is_some() || workspace.is_some() {
                    tracing::warn!("workspace signal bridge attempted to rebind its ready scope");
                    break;
                }
                if asserted_session_id != expected_runtime_session_id {
                    tracing::warn!(
                        "workspace signal bridge ready frame did not match the asserted session"
                    );
                    break;
                }
                member = Some(ready_member);
                workspace = Some(ready_workspace);
            }
            Ok(BridgeFrame::Event(event)) => {
                let Some(workspace) = workspace.clone() else {
                    tracing::warn!("workspace signal bridge delivered an event before ready");
                    break;
                };
                let event_id = event.event_id.clone();
                let (reply, response) = mpsc::channel();
                app_event_tx.send(AppEvent::WorkspaceSignalReceived {
                    request: WorkspaceSignalRequest { event, workspace },
                    reply,
                });
                let outcome = loop {
                    match response.recv_timeout(Duration::from_millis(100)) {
                        Ok(WorkspaceSignalOutcome::Started) => break "started",
                        Ok(WorkspaceSignalOutcome::Held) => break "held",
                        Err(mpsc::RecvTimeoutError::Timeout) if !stop.load(Ordering::Acquire) => {}
                        Err(
                            mpsc::RecvTimeoutError::Timeout | mpsc::RecvTimeoutError::Disconnected,
                        ) => {
                            break "held";
                        }
                    }
                };
                let resolution = Resolution {
                    r#type: "resolve",
                    event_id: &event_id,
                    outcome,
                };
                if serde_json::to_writer(&mut stdin, &resolution).is_err()
                    || stdin.write_all(b"\n").is_err()
                    || stdin.flush().is_err()
                {
                    tracing::warn!("workspace signal bridge resolution write failed");
                    break;
                }
            }
            Ok(BridgeFrame::Continue) => {}
            Err(reason) => {
                tracing::warn!(%reason, "workspace signal bridge frame rejected");
                break;
            }
        }
    }
}

#[derive(Debug)]
enum BridgeFrame {
    Ready {
        asserted_session_id: String,
        member: String,
        workspace: String,
    },
    Event(WorkspaceSignalEvent),
    Continue,
}

fn parse_frame(
    line: &str,
    ready_member: Option<&str>,
    ready_workspace: Option<&str>,
) -> Result<BridgeFrame, &'static str> {
    let value: Value = serde_json::from_str(line).map_err(|_| "invalidJson")?;
    let object = value.as_object().ok_or("invalidFrame")?;
    if object.get("protocol").and_then(Value::as_str) != Some(BRIDGE_PROTOCOL) {
        return Err("protocolMismatch");
    }
    match object.get("type").and_then(Value::as_str) {
        Some("ready") => {
            let asserted_session_id = safe_atom(object.get("assertedSessionId"), 128)?;
            let member = safe_atom(object.get("member"), 64)?;
            safe_atom(object.get("runtimeSessionId"), 128)?;
            let workspace = safe_atom(object.get("workspace"), 64)?;
            Ok(BridgeFrame::Ready {
                asserted_session_id,
                member,
                workspace,
            })
        }
        Some("event") => {
            let Some(ready_member) = ready_member else {
                return Err("eventBeforeReady");
            };
            if ready_workspace.is_none() {
                return Err("eventBeforeReady");
            }
            let event =
                serde_json::from_value(object.get("event").cloned().ok_or("eventUnavailable")?)
                    .map_err(|_| "eventInvalid")?;
            validate_event(&event)?;
            if event.to != ready_member {
                return Err("eventScopeMismatch");
            }
            Ok(BridgeFrame::Event(event))
        }
        Some("resolved") | Some("idle") => Ok(BridgeFrame::Continue),
        Some("hold") => Err("bridgeHeld"),
        _ => Err("frameTypeInvalid"),
    }
}

fn safe_atom(value: Option<&Value>, maximum: usize) -> Result<String, &'static str> {
    let value = value.and_then(Value::as_str).ok_or("metadataInvalid")?;
    if value.is_empty()
        || value.len() > maximum
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'@')
        })
    {
        return Err("metadataInvalid");
    }
    Ok(value.to_string())
}

fn validate_event(event: &WorkspaceSignalEvent) -> Result<(), &'static str> {
    if event.priority > 2 || event.targets.len() > MAX_TARGETS {
        return Err("eventInvalid");
    }
    for value in [
        event.event_id.as_str(),
        event.event_sequence.as_str(),
        event.from.as_str(),
        event.to.as_str(),
    ]
    .into_iter()
    .chain(event.targets.iter().map(String::as_str))
    {
        if safe_atom(Some(&Value::String(value.to_string())), 128).is_err() {
            return Err("eventInvalid");
        }
    }
    if event.signal != WorkspaceSignal::Cc && !event.targets.is_empty() {
        return Err("eventInvalid");
    }
    Ok(())
}

impl App {
    pub(super) fn start_workspace_signal_bridge(&mut self, thread_id: ThreadId) {
        if self
            .workspace_signal_bridge
            .as_ref()
            .is_some_and(|bridge| bridge.serves(thread_id))
        {
            return;
        }
        self.workspace_signal_bridge = None;
        match WorkspaceSignalBridge::start(thread_id, self.app_event_tx.clone()) {
            Ok(bridge) => self.workspace_signal_bridge = bridge,
            Err(err) => tracing::warn!(%thread_id, %err, "failed to start workspace signal bridge"),
        }
    }

    pub(super) async fn handle_workspace_signal(
        &mut self,
        app_server: &mut AppServerSession,
        request: WorkspaceSignalRequest,
        reply: WorkspaceSignalResponseSender,
    ) {
        let Some(primary_thread_id) = self.primary_thread_id else {
            let _ = reply.send(WorkspaceSignalOutcome::Held);
            return;
        };
        let mut reference = format!("workspace-signal/v1/{}", request.event.signal.as_str());
        for target in &request.event.targets {
            reference.push_str("/cc/");
            reference.push_str(target);
        }
        let request_id = app_server.next_request_id();
        let response = app_server
            .request_handle()
            .request_typed::<ThreadAttentionResponse>(ClientRequest::ThreadAttention {
                request_id,
                params: ThreadAttentionParams {
                    thread_id: primary_thread_id.to_string(),
                    attention: ThreadAttentionEvent {
                        version: ATTENTION_VERSION,
                        event_id: request.event.event_id,
                        kind: request.event.signal.attention_kind(),
                        source_class: "workspace".to_string(),
                        source_ref: format!("{}/{}", request.workspace, request.event.from),
                        reference: Some(reference),
                    },
                },
            })
            .await;
        let outcome = match response {
            Ok(ThreadAttentionResponse::Started {}) => WorkspaceSignalOutcome::Started,
            Ok(ThreadAttentionResponse::Held { reason }) => {
                tracing::debug!(
                    reason = match reason {
                        ThreadAttentionHeldReason::PendingTriggerTurn => "pendingOperatorTurn",
                        ThreadAttentionHeldReason::PlanMode => "planMode",
                        ThreadAttentionHeldReason::Busy => "busy",
                    },
                    "workspace signal attention held"
                );
                WorkspaceSignalOutcome::Held
            }
            Err(err) => {
                tracing::warn!(%err, "workspace signal attention outcome is ambiguous");
                WorkspaceSignalOutcome::Held
            }
        };
        let _ = reply.send(outcome);
    }
}

#[cfg(test)]
#[path = "workspace_signal_bridge_tests.rs"]
mod tests;
