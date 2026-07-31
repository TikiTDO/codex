//! Unix socket transport and receiver-policy admission for typed poke signals.

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fs;
use std::io::Read;
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use super::*;

const MAX_FRAME_BYTES: u64 = 4096;
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(50);
const MAX_REPLAY_IDS: usize = 1024;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PokePolicy {
    version: u8,
    recipient: String,
    sources: Vec<PokeSourcePolicy>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PokeSourcePolicy {
    workspace: String,
    member: String,
    #[serde(default)]
    sessions: Vec<String>,
    signals: Vec<PokeSignalPolicy>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PokeSignalPolicy {
    signal: PokeSignal,
    max_events: u32,
    per_seconds: u64,
}

#[derive(Default)]
struct AdmissionState {
    replay: HashMap<(String, String, PokeSignal), ReplayState>,
    quota_events: HashMap<(String, String, PokeSignal), VecDeque<Instant>>,
}

#[derive(Default)]
struct ReplayState {
    order: VecDeque<String>,
    ids: HashSet<String>,
}

impl AdmissionState {
    fn admit(
        &mut self,
        request: &PokeRequest,
        policy: PokeSignalPolicy,
        now: Instant,
    ) -> Result<(), &'static str> {
        let key = (
            request.workspace.clone(),
            request.member.clone(),
            request.signal,
        );
        let replay = self.replay.entry(key.clone()).or_default();
        if replay.ids.contains(&request.event_id) {
            return Err("duplicateEvent");
        }
        let events = self.quota_events.entry(key).or_default();
        let window = Duration::from_secs(policy.per_seconds);
        while events
            .front()
            .is_some_and(|recorded| now.duration_since(*recorded) >= window)
        {
            events.pop_front();
        }
        if events.len() >= policy.max_events as usize {
            // A rate-limited event was not delivered, so its id may retry after the window.
            return Err("rateLimited");
        }
        events.push_back(now);
        replay.ids.insert(request.event_id.clone());
        replay.order.push_back(request.event_id.clone());
        if replay.order.len() > MAX_REPLAY_IDS
            && let Some(expired) = replay.order.pop_front()
        {
            replay.ids.remove(&expired);
        }
        Ok(())
    }
}

pub(super) struct UnixPokeListener {
    socket_path: PathBuf,
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl UnixPokeListener {
    pub(super) fn start(
        codex_home: &Path,
        thread_id: ThreadId,
        app_event_tx: crate::app_event_sender::AppEventSender,
    ) -> io::Result<Option<Self>> {
        let thread_root = codex_home.join("poke").join(thread_id.to_string());
        let policy_path = thread_root.join("policy.json");
        if !policy_path.is_file() {
            return Ok(None);
        }
        fs::create_dir_all(&thread_root)?;
        fs::set_permissions(&thread_root, fs::Permissions::from_mode(0o700))?;
        let socket_path = thread_root.join("poke.sock");
        remove_stale_socket(&socket_path)?;
        let listener = UnixListener::bind(&socket_path)?;
        fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))?;
        listener.set_nonblocking(true)?;

        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let recipient = thread_id.to_string();
        let thread_socket_path = socket_path.clone();
        let worker = thread::Builder::new()
            .name(format!("codex-poke-{}", &recipient[..8]))
            .spawn(move || {
                run_listener(
                    listener,
                    &policy_path,
                    &recipient,
                    app_event_tx,
                    &thread_stop,
                );
                let _ = fs::remove_file(thread_socket_path);
            })?;

        Ok(Some(Self {
            socket_path,
            stop,
            worker: Some(worker),
        }))
    }
}

impl Drop for UnixPokeListener {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = fs::remove_file(&self.socket_path);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn remove_stale_socket(socket_path: &Path) -> io::Result<()> {
    if !socket_path.exists() {
        return Ok(());
    }
    if UnixStream::connect(socket_path).is_ok() {
        return Err(io::Error::new(
            io::ErrorKind::AddrInUse,
            "another client already owns the poke socket",
        ));
    }
    let metadata = fs::symlink_metadata(socket_path)?;
    if metadata.uid() != unsafe { libc::geteuid() } {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "stale poke socket is not owned by this user",
        ));
    }
    fs::remove_file(socket_path)
}

fn run_listener(
    listener: UnixListener,
    policy_path: &Path,
    recipient: &str,
    app_event_tx: crate::app_event_sender::AppEventSender,
    stop: &AtomicBool,
) {
    let mut state = AdmissionState::default();
    while !stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((mut stream, _)) => {
                let response = handle_stream(
                    &mut stream,
                    policy_path,
                    recipient,
                    &app_event_tx,
                    &mut state,
                );
                let _ = serde_json::to_writer(&mut stream, &response);
                let _ = stream.write_all(b"\n");
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(ACCEPT_RETRY_DELAY);
            }
            Err(err) => {
                tracing::warn!(%err, "typed poke listener accept failed");
                thread::sleep(ACCEPT_RETRY_DELAY);
            }
        }
    }
}

fn handle_stream(
    stream: &mut UnixStream,
    policy_path: &Path,
    recipient: &str,
    app_event_tx: &crate::app_event_sender::AppEventSender,
    state: &mut AdmissionState,
) -> PokeResponse {
    if let Err(err) = stream.set_read_timeout(Some(Duration::from_secs(2))) {
        return rejected(format!("streamTimeout:{err}"));
    }
    let mut bytes = Vec::new();
    if let Err(err) = stream.take(MAX_FRAME_BYTES + 1).read_to_end(&mut bytes) {
        return rejected(format!("readFailed:{err}"));
    }
    if bytes.len() as u64 > MAX_FRAME_BYTES {
        return rejected("frameTooLarge");
    }
    let request: PokeRequest = match serde_json::from_slice(&bytes) {
        Ok(request) => request,
        Err(_) => return rejected("invalidRequest"),
    };
    if request.version != POKE_VERSION {
        return rejected("unsupportedVersion");
    }
    if request.to != recipient {
        return rejected("wrongRecipient");
    }
    if !safe_atom(&request.event_id, 128)
        || !safe_atom(&request.workspace, 64)
        || !safe_atom(&request.member, 64)
        || !safe_atom(&request.session_id, 64)
        || !safe_source_ref(&request.workspace, &request.member, &request.session_id)
    {
        return rejected("unsafeMetadata");
    }

    let policy = match read_policy(policy_path, recipient) {
        Ok(policy) => policy,
        Err(reason) => return rejected(reason),
    };
    let Some(signal_policy) = policy
        .sources
        .iter()
        .find(|source| {
            source.workspace == request.workspace
                && source.member == request.member
                && (source.sessions.is_empty() || source.sessions.contains(&request.session_id))
        })
        .and_then(|source| {
            source
                .signals
                .iter()
                .find(|candidate| candidate.signal == request.signal)
        })
        .copied()
    else {
        return rejected("notAllowed");
    };
    if signal_policy.max_events == 0 || signal_policy.per_seconds == 0 {
        return rejected("invalidPolicyQuota");
    }
    if let Err(reason) = state.admit(&request, signal_policy, Instant::now()) {
        return rejected(reason);
    }

    let (reply, response) = mpsc::channel();
    app_event_tx.send(AppEvent::PokeReceived { request, reply });
    match response.recv_timeout(RESPONSE_TIMEOUT) {
        Ok(response) => response,
        Err(mpsc::RecvTimeoutError::Timeout) => PokeResponse::Held {
            reason: "clientResponseTimeout".to_string(),
        },
        Err(mpsc::RecvTimeoutError::Disconnected) => PokeResponse::Held {
            reason: "clientStopped".to_string(),
        },
    }
}

fn read_policy(path: &Path, recipient: &str) -> Result<PokePolicy, &'static str> {
    let metadata = fs::symlink_metadata(path).map_err(|_| "policyUnavailable")?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o022 != 0
    {
        return Err("policyPermissions");
    }
    let bytes = fs::read(path).map_err(|_| "policyUnavailable")?;
    let policy: PokePolicy = serde_json::from_slice(&bytes).map_err(|_| "invalidPolicy")?;
    if policy.version != POKE_VERSION || policy.recipient != recipient {
        return Err("policyRecipientMismatch");
    }
    let mut sources = HashSet::new();
    for source in &policy.sources {
        let source_key = (&source.workspace, &source.member);
        if !safe_atom(&source.workspace, 64)
            || !safe_atom(&source.member, 64)
            || !sources.insert(source_key)
            || source.sessions.iter().any(|session| {
                !safe_atom(session, 64)
                    || !safe_source_ref(&source.workspace, &source.member, session)
            })
        {
            return Err("invalidPolicySource");
        }
        let mut signals = HashSet::new();
        if source.signals.is_empty()
            || source
                .signals
                .iter()
                .any(|entry| !signals.insert(entry.signal))
        {
            return Err("invalidPolicySignals");
        }
    }
    Ok(policy)
}

fn safe_atom(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/' | b'@')
        })
}

fn safe_source_ref(workspace: &str, member: &str, session_id: &str) -> bool {
    workspace.len() + member.len() + session_id.len() + "/@".len() <= 128
}

fn rejected(reason: impl Into<String>) -> PokeResponse {
    PokeResponse::Rejected {
        reason: reason.into(),
    }
}

#[cfg(test)]
#[path = "poke_tests.rs"]
mod tests;
