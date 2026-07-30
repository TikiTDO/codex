use std::collections::VecDeque;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use codex_app_server_protocol::ThreadAttentionKind;
use serde::Deserialize;
use serde::Serialize;

use super::event::AttentionEvent;
use super::event::InboxIdentity;
use super::event::PeriodicAttention;
use super::event::validate_atom;

const LISTENER_STATE_VERSION: u8 = 2;
pub(super) const MAX_SEEN_EVENT_IDS: usize = 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct PendingAttempt {
    pub(super) thread_id: String,
    pub(super) event_id: String,
    pub(super) kind: ThreadAttentionKind,
    pub(super) event_offset: Option<u64>,
    pub(super) next_event_offset: Option<u64>,
    pub(super) attempted_at: i64,
}

impl PendingAttempt {
    pub(super) fn for_event(
        thread_id: &str,
        event_offset: u64,
        next_event_offset: u64,
        event: &AttentionEvent,
        attempted_at: i64,
    ) -> Self {
        Self {
            thread_id: thread_id.to_string(),
            event_id: event.event_id.clone(),
            kind: event.kind.rpc_kind(),
            event_offset: Some(event_offset),
            next_event_offset: Some(next_event_offset),
            attempted_at,
        }
    }

    pub(super) fn for_periodic(
        thread_id: &str,
        event: &PeriodicAttention,
        attempted_at: i64,
    ) -> Self {
        Self {
            thread_id: thread_id.to_string(),
            event_id: event.event_id.clone(),
            kind: ThreadAttentionKind::Periodic,
            event_offset: None,
            next_event_offset: None,
            attempted_at,
        }
    }

    fn validate(&self) -> Result<()> {
        validate_atom("pendingAttempt.threadId", &self.thread_id, 128)?;
        validate_atom("pendingAttempt.eventId", &self.event_id, 128)?;
        match self.kind {
            ThreadAttentionKind::Mention | ThreadAttentionKind::DirectedResponse => {
                let (Some(event_offset), Some(next_event_offset)) =
                    (self.event_offset, self.next_event_offset)
                else {
                    bail!("event pending attempt requires event offsets");
                };
                if next_event_offset <= event_offset {
                    bail!("event pending attempt next offset must advance");
                }
            }
            ThreadAttentionKind::Periodic => {
                if self.event_offset.is_some() || self.next_event_offset.is_some() {
                    bail!("periodic pending attempt must not contain event offsets");
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct ListenerState {
    pub(super) version: u8,
    pub(super) event_offset: u64,
    #[serde(default)]
    pub(super) inbox_identity: Option<InboxIdentity>,
    pub(super) seen_event_ids: VecDeque<String>,
    pub(super) last_periodic_at: Option<i64>,
    pub(super) pending_attempt: Option<PendingAttempt>,
}

impl Default for ListenerState {
    fn default() -> Self {
        Self {
            version: LISTENER_STATE_VERSION,
            event_offset: 0,
            inbox_identity: None,
            seen_event_ids: VecDeque::new(),
            last_periodic_at: None,
            pending_attempt: None,
        }
    }
}

impl ListenerState {
    pub(super) fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let state: Self = serde_json::from_slice(
            &fs::read(path)
                .with_context(|| format!("failed to read listener state {}", path.display()))?,
        )
        .with_context(|| format!("failed to parse listener state {}", path.display()))?;
        if state.version != LISTENER_STATE_VERSION {
            bail!(
                "unsupported listener state version {}; expected {LISTENER_STATE_VERSION}",
                state.version
            );
        }
        if state.event_offset > 0 && state.inbox_identity.is_none() {
            bail!(
                "ATTENTION INBOX HOLD: listener state {} has durable event offset {} but no inbox file identity; refusing to bind a legacy offset to an arbitrary current inbox",
                path.display(),
                state.event_offset,
            );
        }
        if let Some(pending_attempt) = state.pending_attempt.as_ref() {
            pending_attempt.validate()?;
        }
        Ok(state)
    }

    pub(super) fn require_no_pending_attempt(&self) -> Result<()> {
        if let Some(pending_attempt) = self.pending_attempt.as_ref() {
            bail!(
                "AMBIGUOUS EFFECT HOLD: pending {} attention attempt {} for thread {} must be reconciled before restart",
                attention_kind_name(pending_attempt.kind),
                pending_attempt.event_id,
                pending_attempt.thread_id,
            );
        }
        Ok(())
    }

    pub(super) fn begin_attempt(&mut self, pending_attempt: PendingAttempt) -> Result<()> {
        self.require_no_pending_attempt()?;
        pending_attempt.validate()?;
        self.pending_attempt = Some(pending_attempt);
        Ok(())
    }

    pub(super) fn confirm_held(&mut self) -> Result<PendingAttempt> {
        self.pending_attempt
            .take()
            .context("cannot confirm held without a pending attention attempt")
    }

    pub(super) fn confirm_rejected(&mut self) -> Result<PendingAttempt> {
        self.pending_attempt
            .take()
            .context("cannot confirm rejection without a pending attention attempt")
    }

    pub(super) fn confirm_started(&mut self) -> Result<PendingAttempt> {
        let pending_attempt = self
            .pending_attempt
            .take()
            .context("cannot confirm started without a pending attention attempt")?;
        match pending_attempt.kind {
            ThreadAttentionKind::Mention | ThreadAttentionKind::DirectedResponse => {
                let next_event_offset = pending_attempt
                    .next_event_offset
                    .context("event pending attempt is missing its next offset")?;
                self.event_offset = next_event_offset;
                self.mark_seen(pending_attempt.event_id.clone());
                self.last_periodic_at = Some(pending_attempt.attempted_at);
            }
            ThreadAttentionKind::Periodic => {
                self.last_periodic_at = Some(pending_attempt.attempted_at);
            }
        }
        Ok(pending_attempt)
    }

    pub(super) fn contains(&self, event_id: &str) -> bool {
        self.seen_event_ids.iter().any(|seen| seen == event_id)
    }

    pub(super) fn mark_seen(&mut self, event_id: String) {
        self.seen_event_ids.push_back(event_id);
        while self.seen_event_ids.len() > MAX_SEEN_EVENT_IDS {
            self.seen_event_ids.pop_front();
        }
    }

    pub(super) fn save(&self, path: &Path) -> Result<()> {
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty());
        if let Some(parent) = parent {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create listener state directory {}",
                    parent.display()
                )
            })?;
        }
        let temporary = path.with_extension(format!("tmp.{}", std::process::id()));
        let mut options = OpenOptions::new();
        options.create(true).truncate(true).write(true);
        let mut file = options.open(&temporary).with_context(|| {
            format!(
                "failed to create temporary listener state {}",
                temporary.display()
            )
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(fs::Permissions::from_mode(0o600))?;
        }
        file.write_all(&serde_json::to_vec_pretty(self)?)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temporary, path).with_context(|| {
            format!(
                "failed to replace listener state {} with {}",
                path.display(),
                temporary.display()
            )
        })?;
        sync_parent_directory(path)
    }
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::File::open(parent)
        .with_context(|| {
            format!(
                "failed to open listener state directory {} for durability sync",
                parent.display()
            )
        })?
        .sync_all()
        .with_context(|| {
            format!(
                "failed to sync listener state directory {} after rename",
                parent.display()
            )
        })
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> Result<()> {
    bail!("listener state durability requires Unix parent-directory fsync")
}

#[derive(Debug)]
pub(super) struct ListenerStateLock {
    _file: fs::File,
}

#[cfg(unix)]
pub(super) fn acquire_listener_state_lock(state_path: &Path) -> Result<ListenerStateLock> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::PermissionsExt;

    let lock_path = listener_state_lock_path(state_path);
    if let Some(parent) = lock_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create listener lock directory {}",
                parent.display()
            )
        })?;
    }
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("failed to open listener lock {}", lock_path.display()))?;
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    let lock_result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if lock_result == 0 {
        return Ok(ListenerStateLock { _file: file });
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::EWOULDBLOCK) {
        bail!(
            "listener state is already locked by another process: {}",
            state_path.display()
        );
    }
    Err(error).with_context(|| format!("failed to lock listener state {}", state_path.display()))
}

#[cfg(not(unix))]
pub(super) fn acquire_listener_state_lock(_state_path: &Path) -> Result<ListenerStateLock> {
    bail!("codex listen requires Unix flock support for exclusive listener state custody")
}

pub(super) fn listener_state_lock_path(state_path: &Path) -> PathBuf {
    let mut lock_path = state_path.as_os_str().to_os_string();
    lock_path.push(".lock");
    PathBuf::from(lock_path)
}

fn attention_kind_name(kind: ThreadAttentionKind) -> &'static str {
    match kind {
        ThreadAttentionKind::Mention => "mention",
        ThreadAttentionKind::DirectedResponse => "directedResponse",
        ThreadAttentionKind::Periodic => "periodic",
    }
}
