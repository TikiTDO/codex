mod app_server;
mod event;
mod state;

use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use chrono::Utc;
use clap::Parser;
use codex_app_server_client::TypedRequestError;
use codex_app_server_client::app_server_control_socket_path;
use codex_app_server_protocol::ThreadAttentionEvent as RpcAttentionEvent;
use codex_app_server_protocol::ThreadAttentionHeldReason;
use codex_app_server_protocol::ThreadAttentionResponse;
use codex_core::config::find_codex_home;
use codex_utils_absolute_path::AbsolutePathBuf;
use serde_json::json;
use tokio::time::Instant;
use tokio::time::MissedTickBehavior;

use self::app_server::AppServerRequests;
use self::event::PeriodicAttention;
use self::event::read_next_event;
use self::event::validate_atom;
use self::state::ListenerState;
use self::state::PendingAttempt;
use self::state::acquire_listener_state_lock;
use self::state::listener_state_lock_path;

const MIN_PERIODIC_SECONDS: u64 = 60;
const MAX_PERIODIC_SECONDS: u64 = i64::MAX as u64;
const MIN_POLL_MILLIS: u64 = 100;
const HELD_EVENT_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, Parser)]
pub(crate) struct ListenCommand {
    /// Exact thread to subscribe to and wake.
    #[arg(long, value_name = "THREAD_ID")]
    thread_id: String,

    /// Append-only JSONL attention-event inbox.
    #[arg(long, value_name = "FILE")]
    events: Option<PathBuf>,

    /// Durable listener offset, deduplication, and periodic-clock state.
    #[arg(long, value_name = "FILE")]
    state: PathBuf,

    /// Wake an idle thread at this fixed cadence in seconds.
    #[arg(long, value_name = "SECONDS")]
    periodic_seconds: Option<u64>,

    /// Poll interval for the append-only event inbox.
    #[arg(long, default_value_t = 1000, value_name = "MILLISECONDS")]
    poll_millis: u64,

    /// App-server daemon Unix socket. Defaults to the active CODEX_HOME socket.
    #[arg(long, value_name = "PATH")]
    socket: Option<PathBuf>,

    /// Perform one poll cycle and exit. Intended for bounded validation.
    #[arg(long, default_value_t = false)]
    once: bool,
}

pub(crate) async fn run(command: ListenCommand) -> Result<()> {
    validate_command(&command)?;
    let _state_lock = acquire_listener_state_lock(&command.state)?;
    let mut state = ListenerState::load(&command.state)?;
    if let Some(pending_attempt) = state.pending_attempt.as_ref() {
        println!(
            "{}",
            json!({
                "outcome": "ambiguous",
                "threadId": pending_attempt.thread_id,
                "eventId": pending_attempt.event_id,
                "kind": pending_attempt.kind,
                "eventOffset": pending_attempt.event_offset,
                "nextEventOffset": pending_attempt.next_event_offset,
                "attemptedAt": pending_attempt.attempted_at,
            })
        );
        state.require_no_pending_attempt()?;
    }

    let socket_path = match command.socket {
        Some(socket_path) => AbsolutePathBuf::relative_to_current_dir(socket_path)?,
        None => app_server_control_socket_path(&find_codex_home()?)?,
    };
    let (mut requests, mut disconnect_rx) = app_server::connect(socket_path).await?;
    requests
        .subscribe(&command.thread_id)
        .await
        .context("failed to resume and subscribe listener to exact thread")?;

    let now = Utc::now().timestamp();
    if command.periodic_seconds.is_some() && state.last_periodic_at.is_none() {
        state.last_periodic_at = Some(now);
        state.save(&command.state)?;
    }

    println!(
        "{}",
        json!({
            "outcome": "listening",
            "threadId": command.thread_id,
            "events": command.events,
            "state": command.state,
            "periodicSeconds": command.periodic_seconds,
            "subscription": "threadResume",
        })
    );

    let mut held_event_retry_not_before = None;
    let mut held_periodic_retry_not_before = None;
    let mut ticker = tokio::time::interval(Duration::from_millis(command.poll_millis));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            disconnect_reason = &mut disconnect_rx => {
                let disconnect_reason = disconnect_reason
                    .unwrap_or_else(|_| "app-server event monitor stopped unexpectedly".to_string());
                bail!("app-server disconnected: {disconnect_reason}");
            }
            _ = ticker.tick() => {
                let inbox_outcome = if let Some(events_path) = command.events.as_deref() {
                    if held_retry_is_deferred(
                        held_event_retry_not_before,
                        Instant::now(),
                    ) {
                        InboxCycleOutcome::Held
                    } else {
                        let outcome = process_event_inbox(
                            events_path,
                            &command.state,
                            &command.thread_id,
                            &mut state,
                            &mut requests,
                        )
                        .await?;
                        held_event_retry_not_before = matches!(outcome, InboxCycleOutcome::Held)
                            .then(|| Instant::now() + HELD_EVENT_RETRY_DELAY);
                        outcome
                    }
                } else {
                    InboxCycleOutcome::NoEvent
                };
                if inbox_outcome.permits_periodic()
                    && let Some(periodic_seconds) = command.periodic_seconds
                    && !held_retry_is_deferred(
                        held_periodic_retry_not_before,
                        Instant::now(),
                    )
                {
                    let outcome = process_periodic(
                        periodic_seconds,
                        &command.state,
                        &command.thread_id,
                        &mut state,
                        &mut requests,
                    )
                    .await?;
                    held_periodic_retry_not_before =
                        matches!(outcome, PeriodicCycleOutcome::Held)
                            .then(|| Instant::now() + HELD_EVENT_RETRY_DELAY);
                }
                if command.once {
                    break;
                }
            }
        }
    }
    Ok(())
}

fn validate_command(command: &ListenCommand) -> Result<()> {
    validate_atom("threadId", &command.thread_id, 128)?;
    validate_distinct_paths(command.events.as_deref(), &command.state)?;
    if command.events.is_none() && command.periodic_seconds.is_none() {
        bail!("listen requires --events, --periodic-seconds, or both");
    }
    validate_periodic_seconds(command.periodic_seconds)?;
    if command.poll_millis < MIN_POLL_MILLIS {
        bail!("--poll-millis must be at least {MIN_POLL_MILLIS}");
    }
    Ok(())
}

fn validate_periodic_seconds(periodic_seconds: Option<u64>) -> Result<()> {
    if let Some(periodic_seconds) = periodic_seconds {
        if periodic_seconds < MIN_PERIODIC_SECONDS {
            bail!("--periodic-seconds must be at least {MIN_PERIODIC_SECONDS}");
        }
        if periodic_seconds > MAX_PERIODIC_SECONDS {
            bail!("--periodic-seconds must be at most {MAX_PERIODIC_SECONDS}");
        }
    }
    Ok(())
}

fn validate_distinct_paths(events_path: Option<&Path>, state_path: &Path) -> Result<()> {
    let Some(events_path) = events_path else {
        return Ok(());
    };
    let events_path = AbsolutePathBuf::relative_to_current_dir(events_path)
        .context("failed to resolve --events path")?;
    let state_path = AbsolutePathBuf::relative_to_current_dir(state_path)
        .context("failed to resolve --state path")?;
    let lock_path = listener_state_lock_path(&state_path);
    if paths_alias(&events_path, &state_path) || paths_alias(&events_path, &lock_path) {
        bail!("--events must not alias --state or its lock sidecar");
    }
    Ok(())
}

fn paths_alias(left: &Path, right: &Path) -> bool {
    if left == right {
        return true;
    }
    path_for_alias_comparison(left) == path_for_alias_comparison(right)
}

fn path_for_alias_comparison(path: &Path) -> PathBuf {
    if let Ok(canonical) = fs::canonicalize(path) {
        return canonical;
    }
    if let (Some(parent), Some(file_name)) = (path.parent(), path.file_name())
        && let Ok(canonical_parent) = fs::canonicalize(parent)
    {
        return canonical_parent.join(file_name);
    }
    path.to_path_buf()
}

enum ConfirmedAttentionOutcome {
    Started(PendingAttempt),
    Held {
        pending_attempt: PendingAttempt,
        reason: ThreadAttentionHeldReason,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InboxCycleOutcome {
    NoEvent,
    Started,
    Held,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PeriodicCycleOutcome {
    NotDue,
    Started,
    Held,
}

impl InboxCycleOutcome {
    fn permits_periodic(self) -> bool {
        matches!(self, Self::NoEvent)
    }
}

fn held_retry_is_deferred(deadline: Option<Instant>, now: Instant) -> bool {
    deadline.is_some_and(|deadline| now < deadline)
}

async fn perform_attention_attempt(
    state_path: &Path,
    state: &mut ListenerState,
    requests: &mut AppServerRequests,
    pending_attempt: PendingAttempt,
    attention: RpcAttentionEvent,
) -> Result<ConfirmedAttentionOutcome> {
    state.begin_attempt(pending_attempt)?;
    state.save(state_path)?;

    let thread_id = state
        .pending_attempt
        .as_ref()
        .context("pending attention attempt disappeared before request")?
        .thread_id
        .clone();
    match requests.attention(&thread_id, attention).await {
        Ok(ThreadAttentionResponse::Started {}) => {
            let pending_attempt = state.confirm_started()?;
            state.save(state_path)?;
            Ok(ConfirmedAttentionOutcome::Started(pending_attempt))
        }
        Ok(ThreadAttentionResponse::Held { reason }) => {
            let pending_attempt = state.confirm_held()?;
            state.save(state_path)?;
            Ok(ConfirmedAttentionOutcome::Held {
                pending_attempt,
                reason,
            })
        }
        Err(TypedRequestError::Server { source, .. }) => {
            let pending_attempt = state.confirm_rejected()?;
            state.save(state_path)?;
            let error_code = source.code;
            let error_message = source.message;
            let event_id = pending_attempt.event_id.clone();
            println!(
                "{}",
                json!({
                    "outcome": "rejected",
                    "threadId": pending_attempt.thread_id,
                    "eventId": &event_id,
                    "kind": pending_attempt.kind,
                    "error": error_message,
                    "code": error_code,
                })
            );
            bail!("attention request rejected before effect for event {event_id}");
        }
        Err(
            error @ (TypedRequestError::Transport { .. } | TypedRequestError::Deserialize { .. }),
        ) => {
            let pending_attempt = state
                .pending_attempt
                .as_ref()
                .context("ambiguous request lost its pending attempt")?;
            println!(
                "{}",
                json!({
                    "outcome": "ambiguous",
                    "threadId": pending_attempt.thread_id,
                    "eventId": pending_attempt.event_id,
                    "kind": pending_attempt.kind,
                    "eventOffset": pending_attempt.event_offset,
                    "nextEventOffset": pending_attempt.next_event_offset,
                    "attemptedAt": pending_attempt.attempted_at,
                    "error": error.to_string(),
                })
            );
            bail!(
                "AMBIGUOUS EFFECT HOLD: attention admission for event {} is unknown; pending attempt retained",
                pending_attempt.event_id
            );
        }
    }
}

async fn process_event_inbox(
    events_path: &Path,
    state_path: &Path,
    thread_id: &str,
    state: &mut ListenerState,
    requests: &mut AppServerRequests,
) -> Result<InboxCycleOutcome> {
    loop {
        let inbox_read = read_next_event(events_path, state.event_offset, state.inbox_identity)?;
        if state.inbox_identity.is_none() {
            state.inbox_identity = Some(inbox_read.identity);
            state.save(state_path)?;
        }
        let Some(record) = inbox_read.record else {
            return Ok(InboxCycleOutcome::NoEvent);
        };
        let event = match record.event {
            Ok(event) => event,
            Err(error) => {
                println!(
                    "{}",
                    json!({
                        "outcome": "rejected",
                        "offset": record.event_offset,
                        "error": error.to_string(),
                    })
                );
                state.event_offset = record.next_offset;
                state.save(state_path)?;
                continue;
            }
        };
        if state.contains(&event.event_id) {
            println!(
                "{}",
                json!({
                    "outcome": "duplicate",
                    "eventId": event.event_id,
                })
            );
            state.event_offset = record.next_offset;
            state.save(state_path)?;
            continue;
        }
        let pending_attempt = PendingAttempt::for_event(
            thread_id,
            record.event_offset,
            record.next_offset,
            &event,
            Utc::now().timestamp(),
        );
        match perform_attention_attempt(
            state_path,
            state,
            requests,
            pending_attempt,
            event.rpc_event(),
        )
        .await?
        {
            ConfirmedAttentionOutcome::Started(pending_attempt) => {
                println!(
                    "{}",
                    json!({
                        "outcome": "started",
                        "claim": "admissionOnly",
                        "kind": pending_attempt.kind,
                        "eventId": pending_attempt.event_id,
                        "threadId": thread_id,
                    })
                );
                return Ok(InboxCycleOutcome::Started);
            }
            ConfirmedAttentionOutcome::Held {
                pending_attempt,
                reason,
            } => {
                println!(
                    "{}",
                    json!({
                        "outcome": "held",
                        "reason": reason,
                        "kind": pending_attempt.kind,
                        "eventId": pending_attempt.event_id,
                        "threadId": thread_id,
                    })
                );
                return Ok(InboxCycleOutcome::Held);
            }
        }
    }
}

async fn process_periodic(
    periodic_seconds: u64,
    state_path: &Path,
    thread_id: &str,
    state: &mut ListenerState,
    requests: &mut AppServerRequests,
) -> Result<PeriodicCycleOutcome> {
    let now = Utc::now().timestamp();
    let last_periodic_at = state.last_periodic_at.unwrap_or(now);
    if now.saturating_sub(last_periodic_at) < periodic_seconds as i64 {
        return Ok(PeriodicCycleOutcome::NotDue);
    }
    let slot = now.div_euclid(periodic_seconds as i64);
    let event = PeriodicAttention::for_slot(slot);
    let pending_attempt = PendingAttempt::for_periodic(thread_id, &event, now);
    match perform_attention_attempt(
        state_path,
        state,
        requests,
        pending_attempt,
        event.rpc_event(),
    )
    .await?
    {
        ConfirmedAttentionOutcome::Started(pending_attempt) => {
            println!(
                "{}",
                json!({
                    "outcome": "started",
                    "claim": "admissionOnly",
                    "kind": pending_attempt.kind,
                    "eventId": pending_attempt.event_id,
                    "threadId": thread_id,
                })
            );
            Ok(PeriodicCycleOutcome::Started)
        }
        ConfirmedAttentionOutcome::Held {
            pending_attempt,
            reason,
        } => {
            println!(
                "{}",
                json!({
                    "outcome": "held",
                    "kind": pending_attempt.kind,
                    "eventId": pending_attempt.event_id,
                    "threadId": thread_id,
                    "reason": reason,
                })
            );
            Ok(PeriodicCycleOutcome::Held)
        }
    }
}

#[cfg(test)]
#[path = "listen_cmd_tests.rs"]
mod tests;
