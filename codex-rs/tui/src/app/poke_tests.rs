use std::fs;
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::os::unix::fs::symlink;
use std::os::unix::net::UnixStream;
use std::thread;
use std::time::Duration;

use pretty_assertions::assert_eq;
use tempfile::tempdir;

use super::*;
use crate::app_event::AppEvent;
use crate::app_event_sender::AppEventSender;

#[test]
#[cfg(unix)]
fn listener_forwards_one_policy_admitted_typed_signal() -> anyhow::Result<()> {
    let codex_home = tempdir()?;
    let thread_id = ThreadId::new();
    let root = codex_home.path().join("poke").join(thread_id.to_string());
    fs::create_dir_all(&root)?;
    let policy_path = root.join("policy.json");
    fs::write(
        &policy_path,
        serde_json::json!({
            "version": 1,
            "recipient": thread_id.to_string(),
            "sources": [{
                "workspace": "root",
                "member": "rook-mid",
                "signals": [{"signal": "cc", "maxEvents": 2, "perSeconds": 60}]
            }]
        })
        .to_string(),
    )?;
    fs::set_permissions(&policy_path, fs::Permissions::from_mode(0o600))?;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let listener = PokeListener::start(codex_home.path(), thread_id, AppEventSender::new(tx))?
        .expect("policy should enable listener");
    let socket_path = root.join("poke.sock");

    let client = thread::spawn(move || {
        call_socket(
            &socket_path,
            &PokeRequest {
                version: 1,
                event_id: "event-1".to_string(),
                to: thread_id.to_string(),
                workspace: "root".to_string(),
                member: "rook-mid".to_string(),
                session_id: "mid-session".to_string(),
                signal: PokeSignal::Cc,
            },
        )
    });

    let event = rx.blocking_recv().expect("listener should forward event");
    let AppEvent::PokeReceived { request, reply } = event else {
        panic!("expected poke event");
    };
    assert_eq!(
        request,
        PokeRequest {
            version: 1,
            event_id: "event-1".to_string(),
            to: thread_id.to_string(),
            workspace: "root".to_string(),
            member: "rook-mid".to_string(),
            session_id: "mid-session".to_string(),
            signal: PokeSignal::Cc,
        }
    );
    reply.send(PokeResponse::Started)?;
    assert_eq!(
        client.join().expect("client thread should not panic")?,
        PokeResponse::Started
    );
    drop(listener);
    Ok(())
}

#[test]
#[cfg(unix)]
fn listener_enforces_receiver_owned_per_signal_quota() -> anyhow::Result<()> {
    let codex_home = tempdir()?;
    let thread_id = ThreadId::new();
    let root = codex_home.path().join("poke").join(thread_id.to_string());
    fs::create_dir_all(&root)?;
    let policy_path = root.join("policy.json");
    fs::write(
        &policy_path,
        serde_json::json!({
            "version": 1,
            "recipient": thread_id.to_string(),
            "sources": [{
                "workspace": "root",
                "member": "rook-mid",
                "sessions": ["mid-session"],
                "signals": [{"signal": "cc", "maxEvents": 1, "perSeconds": 60}]
            }]
        })
        .to_string(),
    )?;
    fs::set_permissions(&policy_path, fs::Permissions::from_mode(0o600))?;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let listener = PokeListener::start(codex_home.path(), thread_id, AppEventSender::new(tx))?
        .expect("policy should enable listener");
    let socket_path = root.join("poke.sock");

    let first_path = socket_path.clone();
    let first = thread::spawn(move || {
        call_socket(
            &first_path,
            &PokeRequest {
                version: 1,
                event_id: "event-1".to_string(),
                to: thread_id.to_string(),
                workspace: "root".to_string(),
                member: "rook-mid".to_string(),
                session_id: "mid-session".to_string(),
                signal: PokeSignal::Cc,
            },
        )
    });
    let AppEvent::PokeReceived { reply, .. } =
        rx.blocking_recv().expect("first event should be admitted")
    else {
        panic!("expected poke event");
    };
    reply.send(PokeResponse::Started)?;
    assert_eq!(
        first.join().expect("client thread should not panic")?,
        PokeResponse::Started
    );

    assert_eq!(
        call_socket(
            &socket_path,
            &PokeRequest {
                version: 1,
                event_id: "event-2".to_string(),
                to: thread_id.to_string(),
                workspace: "root".to_string(),
                member: "rook-mid".to_string(),
                session_id: "mid-session".to_string(),
                signal: PokeSignal::Cc,
            },
        )?,
        PokeResponse::Rejected {
            reason: "rateLimited".to_string(),
        }
    );
    assert!(rx.try_recv().is_err());
    drop(listener);
    Ok(())
}

#[test]
#[cfg(unix)]
fn listener_is_disabled_without_receiver_policy() -> anyhow::Result<()> {
    let codex_home = tempdir()?;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    assert!(
        PokeListener::start(codex_home.path(), ThreadId::new(), AppEventSender::new(tx))?.is_none()
    );
    Ok(())
}

#[test]
#[cfg(unix)]
fn dropped_listener_cannot_unlink_a_restarted_listener() -> anyhow::Result<()> {
    let codex_home = tempdir()?;
    let thread_id = ThreadId::new();
    let root = codex_home.path().join("poke").join(thread_id.to_string());
    fs::create_dir_all(&root)?;
    let policy_path = root.join("policy.json");
    fs::write(
        &policy_path,
        serde_json::json!({"version": 1, "recipient": thread_id.to_string(), "sources": []})
            .to_string(),
    )?;
    fs::set_permissions(&policy_path, fs::Permissions::from_mode(0o600))?;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

    let first = PokeListener::start(
        codex_home.path(),
        thread_id,
        AppEventSender::new(tx.clone()),
    )?
    .expect("listener should start");
    drop(first);
    let restarted = PokeListener::start(codex_home.path(), thread_id, AppEventSender::new(tx))?
        .expect("listener should restart");
    thread::sleep(ACCEPT_RETRY_DELAY + ACCEPT_RETRY_DELAY);
    assert!(root.join("poke.sock").exists());
    drop(restarted);
    Ok(())
}

#[test]
#[cfg(unix)]
fn policy_rejects_symlink_and_oversized_source_ref() -> anyhow::Result<()> {
    let root = tempdir()?;
    let target = root.path().join("target.json");
    let policy = root.path().join("policy.json");
    fs::write(&target, "{}")?;
    symlink(target, &policy)?;

    assert_eq!(
        read_policy(&policy, "recipient").unwrap_err(),
        "policyPermissions"
    );
    assert!(safe_source_ref("w", "m", &"s".repeat(124)));
    assert!(!safe_source_ref("w", "m", &"s".repeat(125)));
    Ok(())
}

#[test]
#[cfg(unix)]
fn replay_eviction_is_scoped_to_the_quota_source() {
    let mut state = AdmissionState::default();
    let policy = PokeSignalPolicy {
        signal: PokeSignal::Cc,
        max_events: 2_000,
        per_seconds: 60,
    };
    let now = Instant::now();
    let original = admission_request("member-a", "event-a");
    assert_eq!(state.admit(&original, policy, now), Ok(()));
    for index in 0..=MAX_REPLAY_IDS {
        assert_eq!(
            state.admit(
                &admission_request("member-b", &format!("event-b-{index}")),
                policy,
                now,
            ),
            Ok(())
        );
    }
    assert_eq!(state.admit(&original, policy, now), Err("duplicateEvent"));
}

#[test]
#[cfg(unix)]
fn rate_limited_event_can_retry_after_the_receiver_window() {
    let mut state = AdmissionState::default();
    let policy = PokeSignalPolicy {
        signal: PokeSignal::Cc,
        max_events: 1,
        per_seconds: 60,
    };
    let now = Instant::now();
    assert_eq!(
        state.admit(&admission_request("member", "first"), policy, now),
        Ok(())
    );
    let retry = admission_request("member", "retry");
    assert_eq!(state.admit(&retry, policy, now), Err("rateLimited"));
    assert_eq!(
        state.admit(&retry, policy, now + Duration::from_secs(60)),
        Ok(())
    );
}

#[cfg(unix)]
fn admission_request(member: &str, event_id: &str) -> PokeRequest {
    PokeRequest {
        version: 1,
        event_id: event_id.to_string(),
        to: "recipient".to_string(),
        workspace: "workspace".to_string(),
        member: member.to_string(),
        session_id: "session".to_string(),
        signal: PokeSignal::Cc,
    }
}

#[cfg(unix)]
fn call_socket(path: &Path, request: &PokeRequest) -> anyhow::Result<PokeResponse> {
    let mut stream = connect_with_retry(path)?;
    serde_json::to_writer(&mut stream, request)?;
    stream.shutdown(std::net::Shutdown::Write)?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(serde_json::from_str(response.trim())?)
}

#[cfg(unix)]
fn connect_with_retry(path: &Path) -> io::Result<UnixStream> {
    let mut last_error = None;
    for _ in 0..20 {
        match UnixStream::connect(path) {
            Ok(stream) => return Ok(stream),
            Err(err) => last_error = Some(err),
        }
        thread::sleep(Duration::from_millis(10));
    }
    Err(last_error.expect("at least one connection attempt"))
}
