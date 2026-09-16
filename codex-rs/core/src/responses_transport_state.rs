use codex_protocol::ThreadId;
use codex_utils_home_dir::find_codex_home;
use serde::Serialize;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

const SNAPSHOT_SCHEMA: &str = "codex.responses-transport-state/v1";
const SNAPSHOT_DIRECTORY: &str = "responses-transport";

#[derive(Clone, Copy, Debug)]
pub(crate) enum ResponsesTransport {
    Http,
    Websocket,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum ResponsesFailureReason {
    AuthRecovery,
    ConnectionError,
    ConsumerDropped,
    PreviousResponseNotFound,
    RequestError,
    StreamClosed,
    StreamError,
    UpgradeRequired,
}

impl ResponsesFailureReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::AuthRecovery => "auth_recovery",
            Self::ConnectionError => "connection_error",
            Self::ConsumerDropped => "consumer_dropped",
            Self::PreviousResponseNotFound => "previous_response_not_found",
            Self::RequestError => "request_error",
            Self::StreamClosed => "stream_closed",
            Self::StreamError => "stream_error",
            Self::UpgradeRequired => "upgrade_required",
        }
    }
}

impl ResponsesTransport {
    fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Websocket => "websocket",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ResponsesTransportState {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Debug)]
struct Inner {
    path: Option<PathBuf>,
    snapshot: Snapshot,
    temporary_sequence: u64,
}

#[derive(Clone, Debug, Serialize)]
struct Snapshot {
    schema: &'static str,
    thread_id: String,
    process: ProcessIdentity,
    sequence: u64,
    updated_at_unix_ms: u64,
    transport: TransportSnapshot,
    circuit: CircuitSnapshot,
    last_outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
}

#[derive(Clone, Debug, Serialize)]
struct ProcessIdentity {
    pid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    linux_start_ticks: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
struct TransportSnapshot {
    active: &'static str,
    state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_mode: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    input_items: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    body_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    connection_reused: Option<bool>,
}

#[derive(Clone, Debug, Serialize)]
struct CircuitSnapshot {
    state: &'static str,
    consecutive_openings: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_at_unix_ms: Option<u64>,
}

impl ResponsesTransportState {
    pub(crate) fn new(thread_id: ThreadId) -> Self {
        let path = find_codex_home().ok().map(|home| {
            home.join(SNAPSHOT_DIRECTORY)
                .join(format!("{thread_id}.json"))
                .to_path_buf()
        });
        Self::new_with_path(thread_id.to_string(), path)
    }

    pub(crate) fn new_with_path(thread_id: String, path: Option<PathBuf>) -> Self {
        let state = Self {
            inner: Arc::new(Mutex::new(Inner {
                path,
                snapshot: Snapshot {
                    schema: SNAPSHOT_SCHEMA,
                    thread_id,
                    process: ProcessIdentity {
                        pid: std::process::id(),
                        linux_start_ticks: linux_process_start_ticks(std::process::id()),
                    },
                    sequence: 0,
                    updated_at_unix_ms: unix_time_ms(),
                    transport: TransportSnapshot {
                        active: "idle",
                        state: "idle",
                        request_mode: None,
                        input_items: None,
                        body_bytes: None,
                        connection_reused: None,
                    },
                    circuit: CircuitSnapshot {
                        state: "closed",
                        consecutive_openings: 0,
                        retry_at_unix_ms: None,
                    },
                    last_outcome: "initialized",
                    reason: None,
                },
                temporary_sequence: 0,
            })),
        };
        state.publish();
        state
    }

    pub(crate) fn request_started(
        &self,
        transport: ResponsesTransport,
        incremental: bool,
        input_items: usize,
        body_bytes: Option<usize>,
        connection_reused: Option<bool>,
    ) {
        self.update(|snapshot| {
            snapshot.transport = TransportSnapshot {
                active: transport.as_str(),
                state: "requesting",
                request_mode: Some(if incremental { "incremental" } else { "full" }),
                input_items: Some(input_items),
                body_bytes,
                connection_reused,
            };
            snapshot.last_outcome = "prepared";
            snapshot.reason = None;
        });
    }

    pub(crate) fn websocket_connecting(&self) {
        self.update(|snapshot| {
            snapshot.transport = TransportSnapshot {
                active: ResponsesTransport::Websocket.as_str(),
                state: "connecting",
                request_mode: None,
                input_items: None,
                body_bytes: None,
                connection_reused: None,
            };
            snapshot.last_outcome = "connecting";
            snapshot.reason = None;
        });
    }

    pub(crate) fn stream_started(&self, transport: ResponsesTransport) {
        self.update(|snapshot| {
            snapshot.transport.active = transport.as_str();
            snapshot.transport.state = "streaming";
            snapshot.last_outcome = "streaming";
            snapshot.reason = None;
        });
    }

    pub(crate) fn completed(&self, transport: ResponsesTransport) {
        self.update(|snapshot| {
            snapshot.transport.active = transport.as_str();
            snapshot.transport.state = "completed";
            snapshot.last_outcome = "completed";
            snapshot.reason = None;
            if matches!(transport, ResponsesTransport::Websocket) {
                snapshot.circuit = CircuitSnapshot {
                    state: "closed",
                    consecutive_openings: 0,
                    retry_at_unix_ms: None,
                };
            }
        });
    }

    pub(crate) fn failed(&self, transport: ResponsesTransport, reason: ResponsesFailureReason) {
        self.update(|snapshot| {
            snapshot.transport.active = transport.as_str();
            snapshot.transport.state = "failed";
            snapshot.last_outcome = "failed";
            snapshot.reason = Some(reason.as_str());
        });
    }

    pub(crate) fn fallback_opened(&self) {
        self.update(|snapshot| {
            snapshot.transport.active = "http";
            snapshot.transport.state = "fallback";
            snapshot.circuit = CircuitSnapshot {
                state: "open",
                consecutive_openings: 1,
                retry_at_unix_ms: None,
            };
            snapshot.last_outcome = "fallback";
            snapshot.reason = Some("websocket_retry_exhausted");
        });
    }

    pub(crate) fn websocket_retry_requested(&self) {
        self.update(|snapshot| {
            snapshot.circuit.state = "half_open";
            snapshot.circuit.consecutive_openings = snapshot.circuit.consecutive_openings.max(1);
            snapshot.circuit.retry_at_unix_ms = None;
            snapshot.last_outcome = "websocket_retry_ready";
            snapshot.reason = None;
        });
    }

    fn update(&self, apply: impl FnOnce(&mut Snapshot)) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        apply(&mut inner.snapshot);
        inner.snapshot.sequence = inner.snapshot.sequence.saturating_add(1);
        inner.snapshot.updated_at_unix_ms = unix_time_ms();
        publish_locked(&mut inner);
    }

    fn publish(&self) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        publish_locked(&mut inner);
    }
}

pub(crate) fn serialized_json_len(value: &impl Serialize) -> Option<usize> {
    let mut counter = ByteCounter::default();
    serde_json::to_writer(&mut counter, value).ok()?;
    Some(counter.bytes)
}

#[derive(Default)]
struct ByteCounter {
    bytes: usize,
}

impl Write for ByteCounter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.bytes = self.bytes.saturating_add(buffer.len());
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn publish_locked(inner: &mut Inner) {
    let Some(path) = inner.path.as_deref() else {
        return;
    };
    inner.temporary_sequence = inner.temporary_sequence.saturating_add(1);
    let _ = write_snapshot(path, &inner.snapshot, inner.temporary_sequence);
}

fn write_snapshot(
    path: &Path,
    snapshot: &Snapshot,
    temporary_sequence: u64,
) -> std::io::Result<()> {
    let Some(parent) = path.parent() else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "transport snapshot has no parent directory",
        ));
    };
    fs::create_dir_all(parent)?;
    if !fs::symlink_metadata(parent)?.file_type().is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "transport snapshot directory is not a directory",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
    }
    let temporary = parent.join(format!(
        ".{}.{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("transport-state"),
        std::process::id(),
        temporary_sequence,
    ));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(fs::Permissions::from_mode(0o600))?;
        }
        serde_json::to_writer(&mut file, snapshot).map_err(std::io::Error::other)?;
        file.write_all(b"\n")?;
        file.flush()?;
        fs::rename(&temporary, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(target_os = "linux")]
fn linux_process_start_ticks(pid: u32) -> Option<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let after_name = stat.rsplit_once(')')?.1.trim_start();
    after_name.split_whitespace().nth(19)?.parse().ok()
}

#[cfg(not(target_os = "linux"))]
fn linux_process_start_ticks(_pid: u32) -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use tempfile::TempDir;

    #[test]
    fn snapshot_is_atomic_bounded_state_without_error_text() {
        let temp = TempDir::new().expect("temp dir");
        let path = temp.path().join("responses-transport/thread.json");
        let state = ResponsesTransportState::new_with_path(
            "00000000-0000-4000-8000-000000000071".to_string(),
            Some(path.clone()),
        );

        state.request_started(
            ResponsesTransport::Websocket,
            true,
            3,
            Some(1536),
            Some(true),
        );
        state.fallback_opened();

        let value: Value = serde_json::from_slice(&fs::read(&path).expect("snapshot"))
            .expect("valid snapshot json");
        assert_eq!(value["schema"], SNAPSHOT_SCHEMA);
        assert_eq!(value["thread_id"], "00000000-0000-4000-8000-000000000071");
        assert_eq!(value["process"]["pid"], std::process::id());
        assert_eq!(value["transport"]["active"], "http");
        assert_eq!(value["transport"]["request_mode"], "incremental");
        assert_eq!(value["transport"]["body_bytes"], 1536);
        assert_eq!(value["circuit"]["state"], "open");
        assert_eq!(value["circuit"]["consecutive_openings"], 1);
        assert!(value["circuit"].get("retry_at_unix_ms").is_none());
        assert_eq!(value["last_outcome"], "fallback");
        assert_eq!(value["reason"], "websocket_retry_exhausted");

        state.websocket_retry_requested();
        let value: Value = serde_json::from_slice(&fs::read(&path).expect("retry snapshot"))
            .expect("valid retry snapshot json");
        assert_eq!(value["transport"]["active"], "http");
        assert_eq!(value["transport"]["state"], "fallback");
        assert_eq!(value["circuit"]["state"], "half_open");
        assert_eq!(value["circuit"]["consecutive_openings"], 1);
        assert!(value["circuit"].get("retry_at_unix_ms").is_none());
        assert_eq!(value["last_outcome"], "websocket_retry_ready");
        assert!(value.get("reason").is_none());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(temp.path().join("responses-transport"))
                    .expect("snapshot directory")
                    .permissions()
                    .mode()
                    & 0o777,
                0o700,
            );
            assert_eq!(
                fs::metadata(temp.path().join("responses-transport/thread.json"))
                    .expect("snapshot file")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600,
            );
        }
    }

    #[test]
    fn serialized_length_counts_without_retaining_the_body() {
        let value = serde_json::json!({"large": "x".repeat(1024)});
        assert_eq!(
            serialized_json_len(&value),
            Some(serde_json::to_vec(&value).expect("json").len())
        );
    }
}
