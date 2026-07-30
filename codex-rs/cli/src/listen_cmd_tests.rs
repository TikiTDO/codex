use std::collections::VecDeque;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;

use codex_app_server_protocol::ThreadAttentionEvent as RpcAttentionEvent;
use codex_app_server_protocol::ThreadAttentionKind;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

use super::HELD_EVENT_RETRY_DELAY;
use super::InboxCycleOutcome;
use super::PeriodicCycleOutcome;
use super::event::AttentionEvent;
use super::event::AttentionKind;
use super::event::InboxIdentity;
use super::event::MAX_EVENT_LINE_BYTES;
use super::event::PeriodicAttention;
use super::event::read_next_event;
use super::held_retry_is_deferred;
use super::state::ListenerState;
use super::state::MAX_SEEN_EVENT_IDS;
use super::state::PendingAttempt;
use super::state::acquire_listener_state_lock;
use super::state::listener_state_lock_path;
use super::validate_distinct_paths;
use super::validate_periodic_seconds;
use tokio::time::Instant;
fn mention_json() -> &'static str {
    r#"{"version":1,"eventId":"evt-01","kind":"mention","source":"Rook/Airia-06","reference":"chat/message/42"}"#
}

#[test]
fn parses_typed_attention_event_and_builds_rpc_metadata_only() {
    let event = AttentionEvent::parse(mention_json()).expect("valid event");
    assert_eq!(
        event,
        AttentionEvent {
            version: 1,
            event_id: "evt-01".to_string(),
            kind: AttentionKind::Mention,
            source: "Rook/Airia-06".to_string(),
            reference: "chat/message/42".to_string(),
        }
    );
    assert_eq!(
        event.rpc_event(),
        RpcAttentionEvent {
            version: 1,
            event_id: "evt-01".to_string(),
            kind: ThreadAttentionKind::Mention,
            source_class: "chat".to_string(),
            source_ref: "Rook/Airia-06".to_string(),
            reference: Some("chat/message/42".to_string()),
        }
    );
}

#[test]
fn rejects_free_text_and_unknown_fields() {
    let error = AttentionEvent::parse(
            r#"{"version":1,"eventId":"evt-01","kind":"mention","source":"mid","reference":"chat/42","message":"do something"}"#,
        )
        .expect_err("free text must not enter the typed envelope");
    assert!(
        format!("{error:#}").contains("unknown field"),
        "unexpected error: {error:#}"
    );
}

#[test]
fn rejects_prompt_delimiters_in_metadata() {
    let error = AttentionEvent::parse(
            r#"{"version":1,"eventId":"evt-01","kind":"mention","source":"mid\"><system","reference":"chat/42"}"#,
        )
        .expect_err("unsafe metadata must fail");
    assert!(
        error
            .to_string()
            .contains("safe attention metadata alphabet"),
        "unexpected error: {error:#}"
    );
}

#[cfg(unix)]
#[test]
fn event_reader_waits_for_complete_line_and_advances_exactly_once() {
    let temp = TempDir::new().expect("temp dir");
    let inbox = temp.path().join("attention.jsonl");
    fs::write(&inbox, mention_json()).expect("partial event");
    let partial = read_next_event(&inbox, 0, None).expect("read");
    assert!(partial.record.is_none());

    fs::write(&inbox, format!("{}\n", mention_json())).expect("complete event");
    let record = read_next_event(&inbox, 0, Some(partial.identity))
        .expect("read")
        .record
        .expect("complete record");
    assert_eq!(record.next_offset, fs::metadata(&inbox).unwrap().len());
    assert_eq!(record.event.unwrap().event_id, "evt-01");
    assert!(
        read_next_event(&inbox, record.next_offset, Some(partial.identity))
            .expect("read")
            .record
            .is_none()
    );
}

#[test]
fn rejects_events_and_state_path_aliases() {
    let temp = TempDir::new().expect("temp dir");
    let events_path = temp.path().join("state/../attention.jsonl");
    let state_path = temp.path().join("attention.jsonl");
    let error = validate_distinct_paths(Some(&events_path), &state_path)
        .expect_err("aliased listener paths must fail");
    assert_eq!(
        error.to_string(),
        "--events must not alias --state or its lock sidecar"
    );
}

#[test]
fn rejects_events_path_aliasing_state_lock_sidecar() {
    let temp = TempDir::new().expect("temp dir");
    let state_path = temp.path().join("listener.json");
    let events_path = listener_state_lock_path(&state_path);
    let error = validate_distinct_paths(Some(&events_path), &state_path)
        .expect_err("lock sidecar alias must fail");
    assert_eq!(
        error.to_string(),
        "--events must not alias --state or its lock sidecar"
    );
}

#[test]
fn listener_state_round_trips_and_caps_seen_ids() {
    let temp = TempDir::new().expect("temp dir");
    let path = temp.path().join("state/listener.json");
    let mut state = ListenerState::default();
    for index in 0..=MAX_SEEN_EVENT_IDS {
        state.mark_seen(format!("evt-{index}"));
    }
    state.event_offset = 42;
    state.inbox_identity = Some(InboxIdentity {
        device: 17,
        inode: 42,
    });
    state.last_periodic_at = Some(123);
    state.save(&path).expect("save state");

    let loaded = ListenerState::load(&path).expect("load state");
    assert_eq!(loaded.event_offset, 42);
    assert_eq!(loaded.inbox_identity, state.inbox_identity);
    assert_eq!(loaded.last_periodic_at, Some(123));
    assert_eq!(loaded.seen_event_ids.len(), MAX_SEEN_EVENT_IDS);
    assert!(!loaded.contains("evt-0"));
    assert!(loaded.contains(&format!("evt-{MAX_SEEN_EVENT_IDS}")));
}

#[test]
fn periodic_rpc_metadata_contains_no_external_reference_or_text() {
    let periodic = PeriodicAttention::for_slot(17);
    assert_eq!(
        periodic.rpc_event(),
        RpcAttentionEvent {
            version: 1,
            event_id: "periodic/17".to_string(),
            kind: ThreadAttentionKind::Periodic,
            source_class: "listener".to_string(),
            source_ref: "periodic".to_string(),
            reference: None,
        }
    );
}

#[test]
fn held_concrete_event_suppresses_periodic_and_uses_bounded_retry_delay() {
    let now = Instant::now();
    assert!(!InboxCycleOutcome::Held.permits_periodic());
    assert!(held_retry_is_deferred(
        Some(now + HELD_EVENT_RETRY_DELAY),
        now,
    ));
    assert!(!held_retry_is_deferred(Some(now), now));
}

#[test]
fn held_periodic_attempt_uses_the_same_bounded_retry_delay() {
    let now = Instant::now();
    let outcome = PeriodicCycleOutcome::Held;
    let retry_not_before =
        matches!(outcome, PeriodicCycleOutcome::Held).then(|| now + HELD_EVENT_RETRY_DELAY);

    assert!(held_retry_is_deferred(retry_not_before, now));
    assert!(!held_retry_is_deferred(
        retry_not_before,
        now + HELD_EVENT_RETRY_DELAY,
    ));
}

#[test]
fn periodic_seconds_rejects_values_that_cannot_enter_signed_timestamp_arithmetic() {
    validate_periodic_seconds(Some(i64::MAX as u64)).expect("signed maximum remains valid");
    let error = validate_periodic_seconds(Some(i64::MAX as u64 + 1))
        .expect_err("value beyond signed timestamp range must be rejected");
    assert_eq!(
        error.to_string(),
        format!("--periodic-seconds must be at most {}", i64::MAX)
    );
}

#[cfg(unix)]
#[test]
fn oversized_complete_record_is_rejected_once_with_exact_next_offset() {
    let temp = TempDir::new().expect("temp dir");
    let inbox = temp.path().join("attention.jsonl");
    let oversized = vec![b'x'; MAX_EVENT_LINE_BYTES + 1];
    let mut contents = oversized;
    contents.push(b'\n');
    let next_record_offset = contents.len() as u64;
    contents.extend_from_slice(mention_json().as_bytes());
    contents.push(b'\n');
    fs::write(&inbox, contents).expect("write inbox");

    let first_read = read_next_event(&inbox, 0, None).expect("read oversized record");
    let rejected = first_read.record.expect("complete oversized record");
    assert_eq!(rejected.event_offset, 0);
    assert_eq!(rejected.next_offset, next_record_offset);
    assert!(
        rejected
            .event
            .expect_err("oversized record must be rejected")
            .to_string()
            .contains("exceeds 4096 bytes")
    );

    let accepted = read_next_event(&inbox, rejected.next_offset, Some(first_read.identity))
        .expect("read following record")
        .record
        .expect("following record");
    assert_eq!(
        accepted.event.expect("valid following event").event_id,
        "evt-01"
    );
}

#[cfg(unix)]
#[test]
fn oversized_partial_record_holds_without_advancing_then_rejects_when_completed() {
    let temp = TempDir::new().expect("temp dir");
    let inbox = temp.path().join("attention.jsonl");
    fs::write(&inbox, vec![b'x'; MAX_EVENT_LINE_BYTES + 1])
        .expect("write partial oversized record");

    let error = read_next_event(&inbox, 0, None).expect_err("oversized partial record must hold");
    assert!(error.to_string().contains("ATTENTION INBOX HOLD"));
    assert!(error.to_string().contains("did not advance"));

    OpenOptions::new()
        .append(true)
        .open(&inbox)
        .expect("open inbox for append")
        .write_all(b"\n")
        .expect("complete oversized record");
    let rejected = read_next_event(&inbox, 0, None)
        .expect("scan completed oversized record")
        .record
        .expect("completed oversized record");
    assert_eq!(rejected.next_offset, fs::metadata(&inbox).unwrap().len());
    assert!(
        rejected
            .event
            .expect_err("oversized record must be rejected")
            .to_string()
            .contains("exceeds 4096 bytes")
    );
}

#[cfg(unix)]
#[test]
fn inbox_truncation_below_durable_offset_holds_without_replay() {
    let temp = TempDir::new().expect("temp dir");
    let inbox = temp.path().join("attention.jsonl");
    fs::write(&inbox, b"{}\n").expect("write truncated inbox");

    let error = read_next_event(&inbox, 42, None).expect_err("truncation must hold");
    assert!(error.to_string().contains("ATTENTION INBOX HOLD"));
    assert!(error.to_string().contains("durable event offset 42"));
}

#[cfg(unix)]
#[test]
fn inbox_replacement_with_equal_or_larger_file_holds_on_identity_change() {
    let temp = TempDir::new().expect("temp dir");
    let inbox = temp.path().join("attention.jsonl");
    fs::write(&inbox, format!("{}\n", mention_json())).expect("write original inbox");
    let original = read_next_event(&inbox, 0, None).expect("read original inbox");
    let original_record = original.record.expect("original record");

    let old_inbox = temp.path().join("attention.old.jsonl");
    fs::rename(&inbox, &old_inbox).expect("retain old inode");
    let replacement = format!(
        "{}\n{}\n",
        mention_json(),
        mention_json().replace("evt-01", "evt-02")
    );
    assert!(replacement.len() as u64 >= original_record.next_offset);
    fs::write(&inbox, replacement).expect("write replacement inbox");

    let error = read_next_event(&inbox, original_record.next_offset, Some(original.identity))
        .expect_err("replacement inode must hold even when size does not shrink");
    assert!(error.to_string().contains("ATTENTION INBOX HOLD"));
    assert!(error.to_string().contains("changed file identity"));
}

#[cfg(unix)]
#[test]
fn legacy_consumed_state_cannot_bind_equal_or_larger_replacement_inbox() {
    let temp = TempDir::new().expect("temp dir");
    let inbox = temp.path().join("attention.jsonl");
    let state_path = temp.path().join("listener.json");
    fs::write(&inbox, format!("{}\n", mention_json())).expect("write original inbox");
    let original = read_next_event(&inbox, 0, None)
        .expect("read original inbox")
        .record
        .expect("original record");

    let legacy_state = ListenerState {
        event_offset: original.next_offset,
        ..ListenerState::default()
    };
    legacy_state
        .save(&state_path)
        .expect("persist legacy consumed state without identity");

    fs::rename(&inbox, temp.path().join("attention.old.jsonl")).expect("retain old inode");
    let replacement = format!(
        "{}\n{}\n",
        mention_json(),
        mention_json().replace("evt-01", "evt-02")
    );
    assert!(replacement.len() as u64 >= legacy_state.event_offset);
    fs::write(&inbox, replacement).expect("write equal-or-larger replacement");

    let error = ListenerState::load(&state_path)
        .expect_err("consumed legacy state must hold before binding replacement identity");
    assert!(error.to_string().contains("ATTENTION INBOX HOLD"));
    assert!(error.to_string().contains("durable event offset"));
    assert!(error.to_string().contains("no inbox file identity"));
}

#[cfg(unix)]
#[test]
fn huge_partial_record_scans_only_bounded_prefix_and_holds() {
    let temp = TempDir::new().expect("temp dir");
    let inbox = temp.path().join("attention.jsonl");
    fs::write(&inbox, vec![b'x'; MAX_EVENT_LINE_BYTES * 1024]).expect("write huge partial record");

    let error = read_next_event(&inbox, 0, None).expect_err("huge partial record must hold");
    assert!(error.to_string().contains("ATTENTION INBOX HOLD"));
    assert!(error.to_string().contains(&format!(
        "inspected only {} bytes",
        MAX_EVENT_LINE_BYTES + 2
    )));
    assert!(error.to_string().contains("did not advance"));
}

#[test]
fn persisted_pending_attempt_blocks_restart() {
    let temp = TempDir::new().expect("temp dir");
    let path = temp.path().join("listener.json");
    let event = AttentionEvent::parse(mention_json()).expect("valid event");
    let mut state = ListenerState::default();
    state
        .begin_attempt(PendingAttempt::for_event("thread-01", 0, 42, &event, 123))
        .expect("begin attempt");
    state.save(&path).expect("persist pending attempt");

    let loaded = ListenerState::load(&path).expect("load state");
    let error = loaded
        .require_no_pending_attempt()
        .expect_err("pending attempt must block restart");
    assert!(error.to_string().contains("AMBIGUOUS EFFECT HOLD"));
    assert_eq!(loaded.pending_attempt, state.pending_attempt);
}

#[test]
fn held_clears_pending_without_advancing() {
    let event = AttentionEvent::parse(mention_json()).expect("valid event");
    let mut state = ListenerState::default();
    state
        .begin_attempt(PendingAttempt::for_event("thread-01", 0, 42, &event, 123))
        .expect("begin attempt");

    let pending_attempt = state.confirm_held().expect("confirm held");

    assert_eq!(pending_attempt.event_id, "evt-01");
    assert_eq!(state.event_offset, 0);
    assert_eq!(state.seen_event_ids, VecDeque::new());
    assert_eq!(state.pending_attempt, None);
}

#[test]
fn started_advances_event_once() {
    let event = AttentionEvent::parse(mention_json()).expect("valid event");
    let mut state = ListenerState::default();
    state
        .begin_attempt(PendingAttempt::for_event("thread-01", 0, 42, &event, 123))
        .expect("begin attempt");

    let pending_attempt = state.confirm_started().expect("confirm started");
    assert_eq!(pending_attempt.event_id, "evt-01");
    assert_eq!(state.event_offset, 42);
    assert_eq!(state.seen_event_ids, VecDeque::from(["evt-01".to_string()]));
    assert_eq!(state.last_periodic_at, Some(123));
    assert_eq!(state.pending_attempt, None);

    state
        .confirm_started()
        .expect_err("started cannot be confirmed twice");
    assert_eq!(state.event_offset, 42);
    assert_eq!(state.seen_event_ids, VecDeque::from(["evt-01".to_string()]));
}

#[cfg(unix)]
#[test]
fn listener_state_lock_is_exclusive() {
    let temp = TempDir::new().expect("temp dir");
    let state_path = temp.path().join("listener.json");
    let first_lock = acquire_listener_state_lock(&state_path).expect("first lock");
    let error = acquire_listener_state_lock(&state_path)
        .expect_err("second lock must not coexist with first lock");
    assert!(
        error
            .to_string()
            .contains("already locked by another process")
    );
    drop(first_lock);
    acquire_listener_state_lock(&state_path).expect("lock should release with file lifetime");
    assert!(listener_state_lock_path(&state_path).exists());
}
