use pretty_assertions::assert_eq;

use super::*;

fn ready() -> String {
    serde_json::json!({
        "protocol": BRIDGE_PROTOCOL_V1,
        "assertedSessionId": "thread-1",
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
        "protocol": BRIDGE_PROTOCOL_V1,
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

fn v2_event() -> String {
    serde_json::json!({
        "protocol": BRIDGE_PROTOCOL_V2,
        "status": "deliveryPending",
        "type": "event",
        "event": {
            "attentionKind": "directedResponse",
            "batchCount": 3,
            "createdAt": "2026-08-01T00:00:00.000Z",
            "deliveryMode": "queue",
            "eventId": "event-1",
            "eventSequence": "4",
            "firstEventSequence": "4",
            "from": "terra-aurora-05",
            "latestEventSequence": "6",
            "predatesRuntime": false,
            "priority": 0,
            "signal": "tap",
            "sourceCount": 4,
            "sourceFirstRef": "listen:chat:41",
            "sourceLatestRef": "listen:chat:44",
            "targets": [],
            "to": "terra-aurora-05",
            "wakeClass": "operatorChat"
        }
    })
    .to_string()
}

#[test]
fn ready_frame_binds_the_payload_free_source_scope() {
    let BridgeFrame::Ready {
        asserted_session_id,
        member,
        protocol,
        workspace,
    } = parse_frame(
        &ready(),
        /*ready_protocol*/ None,
        /*ready_member*/ None,
        /*ready_workspace*/ None,
    )
    .expect("ready")
    else {
        panic!("expected ready frame");
    };
    assert_eq!(asserted_session_id, "thread-1");
    assert_eq!(member, "rook-left-builder");
    assert_eq!(protocol, BridgeProtocol::V1);
    assert_eq!(workspace, "root");
}

#[test]
fn event_requires_ready_and_preserves_closed_signal_metadata() {
    assert_eq!(
        parse_frame(
            &event(),
            Some(BridgeProtocol::V1),
            /*ready_member*/ None,
            /*ready_workspace*/ None
        )
        .unwrap_err(),
        "eventBeforeReady"
    );
    let BridgeFrame::Event { event, protocol } = parse_frame(
        &event(),
        Some(BridgeProtocol::V1),
        Some("rook-left-builder"),
        Some("root"),
    )
    .expect("event") else {
        panic!("expected event frame");
    };
    assert_eq!(protocol, BridgeProtocol::V1);
    assert_eq!(
        event,
        WorkspaceSignalEvent {
            attention_kind: None,
            batch_count: None,
            created_at: "2026-08-01T00:00:00.000Z".to_string(),
            delivery_mode: "queue".to_string(),
            event_id: "event-1".to_string(),
            event_sequence: "4".to_string(),
            first_event_sequence: None,
            from: "rook-mid-pm".to_string(),
            predates_runtime: false,
            priority: 0,
            signal: WorkspaceSignal::Cc,
            source_count: None,
            source_first_ref: None,
            source_latest_ref: None,
            targets: vec!["right".to_string()],
            to: "rook-left-builder".to_string(),
            latest_event_sequence: None,
            wake_class: None,
        }
    );
}

#[test]
fn event_must_address_the_member_bound_by_ready() {
    assert_eq!(
        parse_frame(
            &event(),
            Some(BridgeProtocol::V1),
            Some("someone-else"),
            Some("root")
        )
        .unwrap_err(),
        "eventScopeMismatch"
    );
}

#[test]
fn v2_operator_chat_uses_the_explicit_directed_kind_and_bounded_batch_reference() {
    let BridgeFrame::Event { event, protocol } = parse_frame(
        &v2_event(),
        Some(BridgeProtocol::V2),
        Some("terra-aurora-05"),
        Some("root"),
    )
    .expect("v2 event") else {
        panic!("expected event frame");
    };
    assert_eq!(protocol, BridgeProtocol::V2);
    assert_eq!(
        event.attention_kind(),
        ThreadAttentionKind::DirectedResponse
    );
    assert_eq!(
        event.reference(protocol),
        "workspace-signal/v2/tap/wake/operatorChat/b/3/e/4-6/s/4/listen:chat:41-listen:chat:44"
    );
}

#[test]
fn v2_rejects_a_cause_kind_mismatch_and_an_unbounded_source() {
    let mut mismatch: Value = serde_json::from_str(&v2_event()).expect("event json");
    mismatch["event"]["attentionKind"] = Value::String("periodic".to_string());
    assert_eq!(
        parse_frame(
            &mismatch.to_string(),
            Some(BridgeProtocol::V2),
            Some("terra-aurora-05"),
            Some("root")
        )
        .unwrap_err(),
        "eventInvalid"
    );

    let mut missing_source: Value = serde_json::from_str(&v2_event()).expect("event json");
    missing_source["event"]["sourceLatestRef"] = Value::Null;
    assert_eq!(
        parse_frame(
            &missing_source.to_string(),
            Some(BridgeProtocol::V2),
            Some("terra-aurora-05"),
            Some("root")
        )
        .unwrap_err(),
        "eventInvalid"
    );
}

#[test]
fn bridge_protocol_cannot_change_after_ready() {
    assert_eq!(
        parse_frame(
            &v2_event(),
            Some(BridgeProtocol::V1),
            Some("terra-aurora-05"),
            Some("root")
        )
        .unwrap_err(),
        "protocolMismatch"
    );
}

#[test]
fn v2_rejects_operator_chat_claimed_by_a_different_membership() {
    let mut peer_claim: Value = serde_json::from_str(&v2_event()).expect("event json");
    peer_claim["event"]["from"] = Value::String("rook-mid-pm".to_string());
    assert_eq!(
        parse_frame(
            &peer_claim.to_string(),
            Some(BridgeProtocol::V2),
            Some("terra-aurora-05"),
            Some("root")
        )
        .unwrap_err(),
        "eventInvalid"
    );
}

#[test]
fn v2_reference_stays_inside_the_app_server_bound_at_maximum_metadata_sizes() {
    let mut maximum: Value = serde_json::from_str(&v2_event()).expect("event json");
    maximum["event"]["batchCount"] = serde_json::json!(100);
    let maximum_sequence = u64::MAX.to_string();
    maximum["event"]["eventSequence"] = Value::String(maximum_sequence.clone());
    maximum["event"]["firstEventSequence"] = Value::String(maximum_sequence.clone());
    maximum["event"]["latestEventSequence"] = Value::String(maximum_sequence);
    maximum["event"]["sourceCount"] = serde_json::json!(100_000_000);
    maximum["event"]["sourceFirstRef"] = Value::String("a".repeat(64));
    maximum["event"]["sourceLatestRef"] = Value::String("b".repeat(64));
    let BridgeFrame::Event { event, protocol } = parse_frame(
        &maximum.to_string(),
        Some(BridgeProtocol::V2),
        Some("terra-aurora-05"),
        Some("root"),
    )
    .expect("maximum v2 event") else {
        panic!("expected event frame");
    };
    assert!(event.reference(protocol).len() <= MAX_REFERENCE_BYTES);
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
