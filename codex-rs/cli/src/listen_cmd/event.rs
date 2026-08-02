use std::fs;
use std::fs::OpenOptions;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use codex_app_server_protocol::ThreadAttentionEvent as RpcAttentionEvent;
use codex_app_server_protocol::ThreadAttentionKind;
use serde::Deserialize;
use serde::Serialize;

pub(super) const ATTENTION_EVENT_VERSION: u8 = 1;
pub(super) const MAX_EVENT_LINE_BYTES: usize = 4096;
const INBOX_READ_BUFFER_BYTES: usize = 1024;
const MAX_INBOX_SCAN_BYTES: usize = MAX_EVENT_LINE_BYTES + 2;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct InboxIdentity {
    pub(super) device: u64,
    pub(super) inode: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) enum AttentionKind {
    Mention,
    DirectedResponse,
}

impl AttentionKind {
    pub(super) fn rpc_kind(self) -> ThreadAttentionKind {
        match self {
            Self::Mention => ThreadAttentionKind::Mention,
            Self::DirectedResponse => ThreadAttentionKind::DirectedResponse,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct AttentionEvent {
    pub(super) version: u8,
    pub(super) event_id: String,
    pub(super) kind: AttentionKind,
    pub(super) source: String,
    pub(super) reference: String,
}

impl AttentionEvent {
    pub(super) fn parse(line: &str) -> Result<Self> {
        if line.len() > MAX_EVENT_LINE_BYTES {
            bail!("attention event exceeds {MAX_EVENT_LINE_BYTES} bytes");
        }
        let event: Self =
            serde_json::from_str(line).context("attention event is not valid typed JSON")?;
        event.validate()?;
        Ok(event)
    }

    fn validate(&self) -> Result<()> {
        if self.version != ATTENTION_EVENT_VERSION {
            bail!(
                "unsupported attention event version {}; expected {ATTENTION_EVENT_VERSION}",
                self.version
            );
        }
        validate_atom("eventId", &self.event_id, 128)?;
        validate_atom("source", &self.source, 128)?;
        validate_atom("reference", &self.reference, 256)?;
        Ok(())
    }

    pub(super) fn rpc_event(&self) -> RpcAttentionEvent {
        RpcAttentionEvent {
            version: self.version,
            event_id: self.event_id.clone(),
            kind: self.kind.rpc_kind(),
            source_class: "chat".to_string(),
            source_ref: self.source.clone(),
            reference: Some(self.reference.clone()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PeriodicAttention {
    pub(super) event_id: String,
}

impl PeriodicAttention {
    pub(super) fn for_slot(slot: i64) -> Self {
        Self {
            event_id: format!("periodic/{slot}"),
        }
    }

    pub(super) fn rpc_event(&self) -> RpcAttentionEvent {
        RpcAttentionEvent {
            version: ATTENTION_EVENT_VERSION,
            event_id: self.event_id.clone(),
            kind: ThreadAttentionKind::Periodic,
            source_class: "listener".to_string(),
            source_ref: "periodic".to_string(),
            reference: None,
        }
    }
}

#[derive(Debug)]
pub(super) struct EventRecord {
    pub(super) event_offset: u64,
    pub(super) next_offset: u64,
    pub(super) event: Result<AttentionEvent>,
}

#[derive(Debug)]
pub(super) struct InboxRead {
    pub(super) identity: InboxIdentity,
    pub(super) record: Option<EventRecord>,
}

pub(super) fn validate_atom(field: &str, value: &str, max_len: usize) -> Result<()> {
    if value.is_empty() || value.len() > max_len {
        bail!("{field} must be between 1 and {max_len} bytes");
    }
    if !value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/' | b'@')
    }) {
        bail!("{field} contains a character outside the safe attention metadata alphabet");
    }
    Ok(())
}

pub(super) fn read_next_event(
    path: &Path,
    offset: u64,
    expected_identity: Option<InboxIdentity>,
) -> Result<InboxRead> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(path)
        .with_context(|| format!("failed to open attention inbox {}", path.display()))?;
    let metadata = file.metadata()?;
    let identity = inbox_identity(&metadata)?;
    if let Some(expected_identity) = expected_identity
        && identity != expected_identity
    {
        bail!(
            "ATTENTION INBOX HOLD: {} changed file identity from device {} inode {} to device {} inode {}; refusing rotation/replacement replay",
            path.display(),
            expected_identity.device,
            expected_identity.inode,
            identity.device,
            identity.inode,
        );
    }
    let file_len = metadata.len();
    if file_len < offset {
        bail!(
            "ATTENTION INBOX HOLD: {} is {} bytes, smaller than durable event offset {offset}; refusing rotation/truncation replay",
            path.display(),
            file_len,
        );
    }
    let mut reader = BufReader::with_capacity(INBOX_READ_BUFFER_BYTES, file);
    reader.seek(SeekFrom::Start(offset))?;
    let mut captured = Vec::with_capacity(MAX_EVENT_LINE_BYTES + 1);
    let mut record_bytes = 0_u64;
    let mut oversized = false;

    loop {
        let (bytes_consumed, payload_bytes, terminated) = {
            let buffer = reader.fill_buf()?;
            if buffer.is_empty() {
                if oversized {
                    bail_partial_event_hold(path, offset, record_bytes)?;
                }
                return Ok(InboxRead {
                    identity,
                    record: None,
                });
            }
            let remaining_scan = MAX_INBOX_SCAN_BYTES.saturating_sub(record_bytes as usize);
            if remaining_scan == 0 {
                bail_partial_event_hold(path, offset, record_bytes)?;
            }
            let inspected = &buffer[..buffer.len().min(remaining_scan)];
            match inspected.iter().position(|byte| *byte == b'\n') {
                Some(newline_index) => (newline_index + 1, newline_index, true),
                None => (inspected.len(), inspected.len(), false),
            }
        };

        if !oversized {
            let buffer = reader.fill_buf()?;
            let remaining_capture = (MAX_EVENT_LINE_BYTES + 1).saturating_sub(captured.len());
            let capture_bytes = payload_bytes.min(remaining_capture);
            captured.extend_from_slice(&buffer[..capture_bytes]);
            oversized = payload_bytes > remaining_capture || captured.len() > MAX_EVENT_LINE_BYTES;
        }
        reader.consume(bytes_consumed);
        record_bytes = record_bytes.saturating_add(bytes_consumed as u64);

        if terminated {
            let next_offset = offset.saturating_add(record_bytes);
            let event = if oversized {
                Err(anyhow::anyhow!(
                    "attention event exceeds {MAX_EVENT_LINE_BYTES} bytes"
                ))
            } else {
                if captured.last() == Some(&b'\r') {
                    captured.pop();
                }
                match std::str::from_utf8(&captured) {
                    Ok("") => Err(anyhow::anyhow!("attention event line is empty")),
                    Ok(line) => AttentionEvent::parse(line),
                    Err(error) => {
                        Err(anyhow::Error::new(error).context("attention event is not valid UTF-8"))
                    }
                }
            };
            return Ok(InboxRead {
                identity,
                record: Some(EventRecord {
                    event_offset: offset,
                    next_offset,
                    event,
                }),
            });
        }
        if record_bytes >= MAX_INBOX_SCAN_BYTES as u64 {
            bail_partial_event_hold(path, offset, record_bytes)?;
        }
    }
}

fn bail_partial_event_hold(path: &Path, offset: u64, inspected_bytes: u64) -> Result<()> {
    bail!(
        "ATTENTION INBOX HOLD: unterminated event at offset {offset} exceeds {MAX_EVENT_LINE_BYTES} bytes in {}; inspected only {inspected_bytes} bytes and did not advance the durable offset",
        path.display(),
    )
}

#[cfg(unix)]
fn inbox_identity(metadata: &fs::Metadata) -> Result<InboxIdentity> {
    use std::os::unix::fs::MetadataExt;

    Ok(InboxIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

#[cfg(not(unix))]
fn inbox_identity(_metadata: &fs::Metadata) -> Result<InboxIdentity> {
    bail!(
        "ATTENTION INBOX HOLD: codex listen requires Unix device/inode identity validation; this platform is unsupported"
    )
}
