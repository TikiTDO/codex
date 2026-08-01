use pretty_assertions::assert_eq;

use super::*;

fn ready() -> String {
    serde_json::json!({
        "protocol": BRIDGE_PROTOCOL,
        "member": "rook-left-builder",
        "runtimeSessionId": "runtime-1",
        "status": "waiting",
        "type": "ready",
        "workspace": "root"
    })
    .to_string()
}

fn event() -> String {
    serde_json::json!({
        "protocol": BRIDGE_PROTOCOL,
        "status": "deliveryPending",
        "type": "event",
        "event": {
            "createdAt": "2026-08-01T00:00:00.000Z",
            "deliveryMode": "queue",
            "eventId": "event-1",
            "eventSequence": "4",
            "from": "rook-mid-pm",
            "predatesRuntime": false,
            "priority": 0,
            "signal": "cc",
            "targets": ["right"],
            "to": "rook-left-builder"
        }
    })
    .to_string()
}

#[test]
fn ready_frame_binds_the_payload_free_source_scope() {
    let BridgeFrame::Ready { member, workspace } = parse_frame(
        &ready(),
        /*ready_member*/ None,
        /*ready_workspace*/ None,
    )
    .expect("ready") else {
        panic!("expected ready frame");
    };
    assert_eq!(member, "rook-left-builder");
    assert_eq!(workspace, "root");
}

#[test]
fn event_requires_ready_and_preserves_closed_signal_metadata() {
    assert_eq!(
        parse_frame(
            &event(),
            /*ready_member*/ None,
            /*ready_workspace*/ None
        )
        .unwrap_err(),
        "eventBeforeReady"
    );
    let BridgeFrame::Event(event) =
        parse_frame(&event(), Some("rook-left-builder"), Some("root")).expect("event")
    else {
        panic!("expected event frame");
    };
    assert_eq!(
        event,
        WorkspaceSignalEvent {
            created_at: "2026-08-01T00:00:00.000Z".to_string(),
            delivery_mode: "queue".to_string(),
            event_id: "event-1".to_string(),
            event_sequence: "4".to_string(),
            from: "rook-mid-pm".to_string(),
            predates_runtime: false,
            priority: 0,
            signal: WorkspaceSignal::Cc,
            targets: vec!["right".to_string()],
            to: "rook-left-builder".to_string(),
        }
    );
}

#[test]
fn resolution_wire_acknowledges_only_started_or_holds() {
    assert_eq!(
        serde_json::to_string(&Resolution {
            r#type: "resolve",
            event_id: "event-1",
            outcome: "started",
        })
        .expect("resolution"),
        r#"{"type":"resolve","eventId":"event-1","outcome":"started"}"#
    );
    assert_eq!(
        serde_json::to_string(&Resolution {
            r#type: "resolve",
            event_id: "event-1",
            outcome: "held",
        })
        .expect("resolution"),
        r#"{"type":"resolve","eventId":"event-1","outcome":"held"}"#
    );
}
