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
use std::process::ChildStdin;
use std::process::ChildStdout;
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
const BRIDGE_PROTOCOL_V1: &str = "observatory.workspace_signal_bridge.v1";
const BRIDGE_PROTOCOL_V2: &str = "observatory.workspace_signal_bridge.v2";
const ATTENTION_VERSION: u8 = 1;
const GLOBAL_CC_WORKSPACE: &str = "global-cc-bootstrap";
const MAX_FRAME_BYTES: usize = 16 * 1024;
const MAX_REFERENCE_BYTES: usize = 256;
const MAX_TARGETS: usize = 16;
const RESTART_DELAY: Duration = Duration::from_secs(1);
const RESTART_POLL_DELAY: Duration = Duration::from_millis(100);

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BridgeProtocol {
    V1,
    V2,
}

impl BridgeProtocol {
    fn parse(value: Option<&Value>) -> Result<Self, &'static str> {
        match value.and_then(Value::as_str) {
            Some(BRIDGE_PROTOCOL_V1) => Ok(Self::V1),
            Some(BRIDGE_PROTOCOL_V2) => Ok(Self::V2),
            _ => Err("protocolMismatch"),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) enum WorkspaceAttentionKind {
    Periodic,
    Mention,
    DirectedResponse,
}

impl WorkspaceAttentionKind {
    fn into_thread_attention(self) -> ThreadAttentionKind {
        match self {
            Self::Periodic => ThreadAttentionKind::Periodic,
            Self::Mention => ThreadAttentionKind::Mention,
            Self::DirectedResponse => ThreadAttentionKind::DirectedResponse,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) enum WorkspaceWakeClass {
    Unspecified,
    OperatorChat,
    PeerMention,
    PeriodicReview,
    Manual,
}

impl WorkspaceWakeClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::OperatorChat => "operatorChat",
            Self::PeerMention => "peerMention",
            Self::PeriodicReview => "periodicReview",
            Self::Manual => "manual",
        }
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct WorkspaceSignalEvent {
    pub(crate) attention_kind: Option<WorkspaceAttentionKind>,
    pub(crate) batch_count: Option<u16>,
    pub(crate) created_at: String,
    pub(crate) delivery_mode: String,
    pub(crate) event_id: String,
    pub(crate) event_sequence: String,
    pub(crate) first_event_sequence: Option<String>,
    pub(crate) from: String,
    pub(crate) predates_runtime: bool,
    pub(crate) priority: u8,
    pub(crate) signal: WorkspaceSignal,
    pub(crate) source_count: Option<u64>,
    pub(crate) source_first_ref: Option<String>,
    pub(crate) source_latest_ref: Option<String>,
    pub(crate) targets: Vec<String>,
    pub(crate) to: String,
    pub(crate) latest_event_sequence: Option<String>,
    pub(crate) wake_class: Option<WorkspaceWakeClass>,
}

impl WorkspaceSignalEvent {
    fn attention_kind(&self) -> ThreadAttentionKind {
        self.attention_kind
            .map(WorkspaceAttentionKind::into_thread_attention)
            .unwrap_or_else(|| self.signal.attention_kind())
    }

    fn reference(&self, protocol: BridgeProtocol) -> String {
        let mut reference = format!(
            "workspace-signal/{}/{}",
            match protocol {
                BridgeProtocol::V1 => "v1",
                BridgeProtocol::V2 => "v2",
            },
            self.signal.as_str()
        );
        if protocol == BridgeProtocol::V2 {
            let wake_class = self
                .wake_class
                .expect("validated v2 event has a wake class");
            let batch_count = self
                .batch_count
                .expect("validated v2 event has a batch count");
            let first = self
                .first_event_sequence
                .as_deref()
                .expect("validated v2 event has a first sequence");
            let latest = self
                .latest_event_sequence
                .as_deref()
                .expect("validated v2 event has a latest sequence");
            reference.push_str(&format!(
                "/wake/{}/b/{batch_count}/e/{first}-{latest}",
                wake_class.as_str()
            ));
            if let (Some(source_count), Some(source_first), Some(source_latest)) = (
                self.source_count,
                self.source_first_ref.as_deref(),
                self.source_latest_ref.as_deref(),
            ) && source_count > 0
            {
                reference.push_str(&format!("/s/{source_count}/{source_first}-{source_latest}"));
            }
        }
        let targets = self
            .targets
            .iter()
            .map(|target| format!("/cc/{target}"))
            .collect::<String>();
        if reference.len() + targets.len() <= MAX_REFERENCE_BYTES {
            reference.push_str(&targets);
        } else {
            reference.push_str(&format!("/cc-count/{}", self.targets.len()));
        }
        debug_assert!(reference.len() <= MAX_REFERENCE_BYTES);
        reference
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct WorkspaceSignalRequest {
    pub(crate) event: WorkspaceSignalEvent,
    protocol: BridgeProtocol,
    pub(crate) workspace: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkspaceSignalOutcome {
    Started,
    Held,
}

pub(crate) type WorkspaceSignalResponseSender = mpsc::Sender<WorkspaceSignalOutcome>;

fn cc_poke_indicator(
    request: &WorkspaceSignalRequest,
    outcome: WorkspaceSignalOutcome,
) -> Option<String> {
    if outcome != WorkspaceSignalOutcome::Started
        || request.protocol != BridgeProtocol::V1
        || request.workspace != GLOBAL_CC_WORKSPACE
        || request.event.signal != WorkspaceSignal::Cc
        || !is_safe_atom(&request.event.from, 128)
    {
        return None;
    }

    let queued_at = chrono::DateTime::parse_from_rfc3339(&request.event.created_at)
        .ok()
        .map(|created_at| {
            created_at
                .with_timezone(&chrono::Utc)
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
        });
    let timing = match (queued_at, request.event.predates_runtime) {
        (Some(queued_at), true) => format!(" (queued {queued_at}; delayed/pre-runtime)"),
        (Some(queued_at), false) => format!(" (queued {queued_at})"),
        (None, true) => " (delayed/pre-runtime)".to_string(),
        (None, false) => String::new(),
    };

    Some(format!(
        "CC poke received now from reported sender {}{timing}. Fresh-state retrieval start is not yet evidenced; delivery and model-turn start do not establish comprehension, acceptance, CC read, or action.",
        request.event.from
    ))
}

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

        Self::start_program(thread_id, app_event_tx, program).map(Some)
    }

    fn start_program(
        thread_id: ThreadId,
        app_event_tx: crate::app_event_sender::AppEventSender,
        program: PathBuf,
    ) -> io::Result<Self> {
        let runtime_session_id = thread_id.to_string();
        let spawned = spawn_bridge(&program, &runtime_session_id)?;
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let child = Arc::new(Mutex::new(Some(spawned.child)));
        let worker_child = Arc::clone(&child);
        let worker = match thread::Builder::new()
            .name(format!("workspace-signal-{}", &thread_id.to_string()[..8]))
            .spawn(move || {
                supervise_bridge(
                    program,
                    spawned.stdout,
                    spawned.stdin,
                    app_event_tx,
                    runtime_session_id,
                    worker_child,
                    worker_stop,
                );
            }) {
            Ok(worker) => worker,
            Err(err) => {
                terminate_child(&child);
                return Err(err);
            }
        };
        Ok(Self {
            thread_id,
            child,
            stop,
            worker: Some(worker),
        })
    }

    pub(crate) fn serves(&self, thread_id: ThreadId) -> bool {
        self.thread_id == thread_id
    }
}

struct SpawnedBridge {
    child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
}

fn spawn_bridge(program: &std::path::Path, runtime_session_id: &str) -> io::Result<SpawnedBridge> {
    let mut child = Command::new(program)
        .args(["stream", "--session-id", runtime_session_id])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;
    let Some(stdin) = child.stdin.take() else {
        let _ = child.kill();
        let _ = child.wait();
        return Err(io::Error::other("workspace bridge stdin is unavailable"));
    };
    let Some(stdout) = child.stdout.take() else {
        let _ = child.kill();
        let _ = child.wait();
        return Err(io::Error::other("workspace bridge stdout is unavailable"));
    };
    Ok(SpawnedBridge {
        child,
        stdin,
        stdout,
    })
}

fn supervise_bridge(
    program: PathBuf,
    mut stdout: ChildStdout,
    mut stdin: ChildStdin,
    app_event_tx: crate::app_event_sender::AppEventSender,
    runtime_session_id: String,
    child: Arc<Mutex<Option<Child>>>,
    stop: Arc<AtomicBool>,
) {
    loop {
        run_bridge(
            stdout,
            stdin,
            app_event_tx.clone(),
            &runtime_session_id,
            &stop,
        );
        terminate_child(&child);
        if stop.load(Ordering::Acquire) {
            break;
        }

        tracing::warn!(
            %runtime_session_id,
            "workspace signal bridge ended unexpectedly; restarting"
        );
        loop {
            if wait_for_restart(&stop) {
                return;
            }
            match spawn_bridge(&program, &runtime_session_id) {
                Ok(spawned) => {
                    let Ok(mut active_child) = child.lock() else {
                        tracing::warn!(
                            "workspace signal bridge child lock was poisoned during restart"
                        );
                        let mut spawned_child = spawned.child;
                        let _ = spawned_child.kill();
                        let _ = spawned_child.wait();
                        return;
                    };
                    if stop.load(Ordering::Acquire) {
                        let mut spawned_child = spawned.child;
                        let _ = spawned_child.kill();
                        let _ = spawned_child.wait();
                        return;
                    }
                    *active_child = Some(spawned.child);
                    stdout = spawned.stdout;
                    stdin = spawned.stdin;
                    break;
                }
                Err(err) => {
                    tracing::warn!(
                        %runtime_session_id,
                        %err,
                        "failed to restart workspace signal bridge"
                    );
                }
            }
        }
    }
}

fn wait_for_restart(stop: &AtomicBool) -> bool {
    let mut waited = Duration::ZERO;
    while waited < RESTART_DELAY {
        if stop.load(Ordering::Acquire) {
            return true;
        }
        let delay = (RESTART_DELAY - waited).min(RESTART_POLL_DELAY);
        thread::sleep(delay);
        waited += delay;
    }
    stop.load(Ordering::Acquire)
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
    let mut protocol = None;
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
        match parse_frame(&line, protocol, member.as_deref(), workspace.as_deref()) {
            Ok(BridgeFrame::Ready {
                asserted_session_id,
                member: ready_member,
                protocol: ready_protocol,
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
                protocol = Some(ready_protocol);
                member = Some(ready_member);
                workspace = Some(ready_workspace);
            }
            Ok(BridgeFrame::Event {
                event,
                protocol: event_protocol,
            }) => {
                let Some(workspace) = workspace.clone() else {
                    tracing::warn!("workspace signal bridge delivered an event before ready");
                    break;
                };
                let event_id = event.event_id.clone();
                let (reply, response) = mpsc::channel();
                app_event_tx.send(AppEvent::WorkspaceSignalReceived {
                    request: WorkspaceSignalRequest {
                        event,
                        protocol: event_protocol,
                        workspace,
                    },
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
        protocol: BridgeProtocol,
        workspace: String,
    },
    Event {
        event: WorkspaceSignalEvent,
        protocol: BridgeProtocol,
    },
    Continue,
}

fn parse_frame(
    line: &str,
    ready_protocol: Option<BridgeProtocol>,
    ready_member: Option<&str>,
    ready_workspace: Option<&str>,
) -> Result<BridgeFrame, &'static str> {
    let value: Value = serde_json::from_str(line).map_err(|_| "invalidJson")?;
    let object = value.as_object().ok_or("invalidFrame")?;
    let protocol = BridgeProtocol::parse(object.get("protocol"))?;
    if ready_protocol.is_some_and(|ready| ready != protocol) {
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
                protocol,
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
            validate_event(&event, protocol)?;
            if event.to != ready_member {
                return Err("eventScopeMismatch");
            }
            Ok(BridgeFrame::Event { event, protocol })
        }
        Some("resolved") | Some("idle") => Ok(BridgeFrame::Continue),
        Some("hold") => Err("bridgeHeld"),
        _ => Err("frameTypeInvalid"),
    }
}

fn safe_atom(value: Option<&Value>, maximum: usize) -> Result<String, &'static str> {
    let value = value.and_then(Value::as_str).ok_or("metadataInvalid")?;
    if !is_safe_atom(value, maximum) {
        return Err("metadataInvalid");
    }
    Ok(value.to_string())
}

fn is_safe_atom(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'@')
        })
}

fn validate_event(
    event: &WorkspaceSignalEvent,
    protocol: BridgeProtocol,
) -> Result<(), &'static str> {
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
    match protocol {
        BridgeProtocol::V1 => {
            if event.attention_kind.is_some()
                || event.batch_count.is_some()
                || event.first_event_sequence.is_some()
                || event.latest_event_sequence.is_some()
                || event.source_count.is_some()
                || event.source_first_ref.is_some()
                || event.source_latest_ref.is_some()
                || event.wake_class.is_some()
            {
                return Err("eventInvalid");
            }
        }
        BridgeProtocol::V2 => validate_v2_event(event)?,
    }
    Ok(())
}

fn validate_v2_event(event: &WorkspaceSignalEvent) -> Result<(), &'static str> {
    let attention_kind = event.attention_kind.ok_or("eventInvalid")?;
    let batch_count = event.batch_count.ok_or("eventInvalid")?;
    if !(1..=100).contains(&batch_count) {
        return Err("eventInvalid");
    }
    let first = bounded_sequence(event.first_event_sequence.as_deref())?;
    let latest = bounded_sequence(event.latest_event_sequence.as_deref())?;
    if first != bounded_sequence(Some(&event.event_sequence))? || latest < first {
        return Err("eventInvalid");
    }
    let wake_class = event.wake_class.ok_or("eventInvalid")?;
    if wake_class == WorkspaceWakeClass::OperatorChat
        && (event.from != event.to || event.signal != WorkspaceSignal::Tap)
    {
        return Err("eventInvalid");
    }
    let expected_attention = match wake_class {
        WorkspaceWakeClass::OperatorChat => WorkspaceAttentionKind::DirectedResponse,
        WorkspaceWakeClass::PeerMention => WorkspaceAttentionKind::Mention,
        WorkspaceWakeClass::Unspecified
        | WorkspaceWakeClass::PeriodicReview
        | WorkspaceWakeClass::Manual => match event.signal {
            WorkspaceSignal::Tap => WorkspaceAttentionKind::Periodic,
            WorkspaceSignal::Cc => WorkspaceAttentionKind::DirectedResponse,
            WorkspaceSignal::Council => WorkspaceAttentionKind::Mention,
        },
    };
    if attention_kind != expected_attention {
        return Err("eventInvalid");
    }
    let source_count = event.source_count.ok_or("eventInvalid")?;
    match wake_class {
        WorkspaceWakeClass::Unspecified => {
            if source_count != 0
                || event.source_first_ref.is_some()
                || event.source_latest_ref.is_some()
            {
                return Err("eventInvalid");
            }
        }
        _ => {
            if !(1..=100_000_000).contains(&source_count) {
                return Err("eventInvalid");
            }
            let source_first = event.source_first_ref.as_deref().ok_or("eventInvalid")?;
            let source_latest = event.source_latest_ref.as_deref().ok_or("eventInvalid")?;
            if !is_safe_atom(source_first, 64)
                || !is_safe_atom(source_latest, 64)
                || source_namespace(source_first) != source_namespace(source_latest)
                || source_namespace(source_first).is_none()
            {
                return Err("eventInvalid");
            }
        }
    }
    Ok(())
}

fn source_namespace(reference: &str) -> Option<&str> {
    let (namespace, position) = reference.rsplit_once(':')?;
    (!namespace.is_empty() && !position.is_empty()).then_some(namespace)
}

fn bounded_sequence(value: Option<&str>) -> Result<u64, &'static str> {
    let value = value.ok_or("eventInvalid")?;
    if value.is_empty() || value.len() > 20 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("eventInvalid");
    }
    value.parse().map_err(|_| "eventInvalid")
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
        let reference = request.event.reference(request.protocol);
        let attention_kind = request.event.attention_kind();
        let request_id = app_server.next_request_id();
        let response = app_server
            .request_handle()
            .request_typed::<ThreadAttentionResponse>(ClientRequest::ThreadAttention {
                request_id,
                params: ThreadAttentionParams {
                    thread_id: primary_thread_id.to_string(),
                    attention: ThreadAttentionEvent {
                        version: ATTENTION_VERSION,
                        event_id: request.event.event_id.clone(),
                        kind: attention_kind,
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
        if let Some(message) = cc_poke_indicator(&request, outcome) {
            self.chat_widget.add_info_message(message, /*hint*/ None);
        }
        let _ = reply.send(outcome);
    }
}

#[cfg(test)]
#[path = "workspace_signal_bridge_tests.rs"]
mod tests;
