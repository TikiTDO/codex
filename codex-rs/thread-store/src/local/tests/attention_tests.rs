use super::*;
use codex_protocol::attention::{
    ThreadAttentionHeldReason, ThreadAttentionNotification, ThreadAttentionOrigin,
    ThreadAttentionOriginKind, ThreadAttentionResponse,
};
use std::io::Write;

fn notice(thread_id: ThreadId) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::ThreadAttention(ThreadAttentionNotification {
        thread_id: thread_id.to_string(),
        batch_id: "receipt".into(),
        origins: vec![ThreadAttentionOrigin {
            kind: ThreadAttentionOriginKind::Peer,
            source_ref: "fixture:one".into(),
            sender: None,
        }],
        admission: ThreadAttentionResponse::Held {
            reason: ThreadAttentionHeldReason::AdmissionUncertain,
        },
    }))
}

#[tokio::test]
async fn canonical_attention_first_read_materializes_only_acquired_new_lifecycle() {
    for mode in [ThreadHistoryMode::Legacy, ThreadHistoryMode::Paginated] {
        let home = TempDir::new().unwrap();
        let store = LocalThreadStore::new(test_config(home.path()), None);
        let id = ThreadId::default();
        assert!(store.load_canonical_attention_receipts(id).await.is_err());
        let mut params = create_thread_params(id);
        params.history_mode = mode;
        store.create_thread(params).await.unwrap();
        let path = store.live_rollout_path(id).await.unwrap();
        assert!(!path.exists());
        let recovered = store.load_canonical_attention_receipts(id).await.unwrap();
        assert_eq!(recovered.thread_id, id);
        assert!(recovered.notices.is_empty());
        assert_eq!(recovered.coverage.len(), 1);
        assert!(recovered.coverage[0].end_byte_offset > 0);
        assert_eq!(
            codex_rollout::read_session_meta_line(&path)
                .await
                .unwrap()
                .meta
                .attention_receipt_lifetime_version,
            Some(1)
        );
        store.discard_thread(id).await.unwrap();
        assert!(store.load_canonical_attention_receipts(id).await.is_err());
    }
}

#[tokio::test]
async fn canonical_attention_cold_resume_covers_durable_receipts_without_sqlite() {
    for mode in [ThreadHistoryMode::Legacy, ThreadHistoryMode::Paginated] {
        let home = TempDir::new().unwrap();
        let id = ThreadId::default();
        let store = LocalThreadStore::new(test_config(home.path()), None);
        let mut params = create_thread_params(id);
        params.history_mode = mode;
        store.create_thread(params).await.unwrap();
        store
            .append_items(AppendThreadItemsParams {
                thread_id: id,
                items: vec![notice(id)],
            })
            .await
            .unwrap();
        let path = store.live_rollout_path(id).await.unwrap();
        store.shutdown_thread(id).await.unwrap();
        let cold = LocalThreadStore::new(test_config(home.path()), None);
        cold.resume_thread(ResumeThreadParams {
            thread_id: id,
            rollout_path: Some(path),
            history: None,
            history_revision: None,
            include_archived: false,
            metadata: thread_metadata(),
        })
        .await
        .unwrap();
        let recovered = cold.load_canonical_attention_receipts(id).await.unwrap();
        assert_eq!(recovered.notices.len(), 1);
        assert_eq!(recovered.notices[0].batch_id, "receipt");
    }
}

#[tokio::test]
async fn canonical_attention_damaged_middle_and_truncation_refuse_but_model_history_is_tolerant() {
    for damage in ["{broken}\n", "{truncated"] {
        let home = TempDir::new().unwrap();
        let id = ThreadId::default();
        let store = LocalThreadStore::new(test_config(home.path()), None);
        store.create_thread(create_thread_params(id)).await.unwrap();
        store
            .persist_thread(id, PersistContext::Standard)
            .await
            .unwrap();
        let path = store.live_rollout_path(id).await.unwrap();
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(damage.as_bytes()).unwrap();
        if damage.ends_with('\n') {
            // A valid record after the damaged middle must not hide the corruption.
            store
                .append_items(AppendThreadItemsParams {
                    thread_id: id,
                    items: vec![notice(id)],
                })
                .await
                .unwrap();
        }
        assert!(store.load_canonical_attention_receipts(id).await.is_err());
        assert!(
            store
                .load_history(LoadThreadHistoryParams {
                    thread_id: id,
                    include_archived: false
                })
                .await
                .is_ok()
        );
    }
}

#[tokio::test]
async fn canonical_attention_copied_fork_preserves_header_and_parent_receipts_as_data() {
    let home = TempDir::new().unwrap();
    let parent = ThreadId::default();
    let child = ThreadId::default();
    let store = LocalThreadStore::new(test_config(home.path()), None);
    store
        .create_thread(create_thread_params(parent))
        .await
        .unwrap();
    store
        .append_items(AppendThreadItemsParams {
            thread_id: parent,
            items: vec![notice(parent)],
        })
        .await
        .unwrap();
    let inherited = store
        .load_history(LoadThreadHistoryParams {
            thread_id: parent,
            include_archived: false,
        })
        .await
        .unwrap();
    let mut params = create_thread_params(child);
    params.forked_from_id = Some(parent);
    store.create_thread(params).await.unwrap();
    store
        .append_items(AppendThreadItemsParams {
            thread_id: child,
            items: inherited.items.clone(),
        })
        .await
        .unwrap();
    let recovered = store
        .load_canonical_attention_receipts(child)
        .await
        .unwrap();
    assert_eq!(recovered.thread_id, child);
    assert!(
        recovered
            .notices
            .iter()
            .all(|notice| notice.thread_id == parent.to_string())
    );
}

#[tokio::test]
async fn canonical_attention_unknown_lifetime_refuses_while_foreground_remains_readable() {
    for version in [None, Some(2)] {
        for mode in [ThreadHistoryMode::Legacy, ThreadHistoryMode::Paginated] {
            let home = TempDir::new().unwrap();
            let id = ThreadId::new();
            let store = LocalThreadStore::new(test_config(home.path()), None);
            let mut params = create_thread_params(id);
            params.history_mode = mode;
            store.create_thread(params).await.unwrap();
            store
                .append_items(AppendThreadItemsParams {
                    thread_id: id,
                    items: vec![notice(id)],
                })
                .await
                .unwrap();
            let path = store.live_rollout_path(id).await.unwrap();
            store.shutdown_thread(id).await.unwrap();
            let text = std::fs::read_to_string(&path).unwrap();
            let (head, rest) = text.split_once('\n').unwrap();
            let mut head: serde_json::Value = serde_json::from_str(head).unwrap();
            match version {
                Some(version) => {
                    head["payload"]["attention_receipt_lifetime_version"] =
                        serde_json::json!(version);
                }
                None => {
                    head["payload"]
                        .as_object_mut()
                        .unwrap()
                        .remove("attention_receipt_lifetime_version");
                }
            }
            std::fs::write(&path, format!("{head}\n{rest}")).unwrap();
            let cold = LocalThreadStore::new(test_config(home.path()), None);
            cold.resume_thread(ResumeThreadParams {
                thread_id: id,
                rollout_path: Some(path),
                history: None,
                history_revision: None,
                include_archived: false,
                metadata: thread_metadata(),
            })
            .await
            .unwrap();
            let err = cold
                .load_canonical_attention_receipts(id)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("receipt lifetime version"));
            let foreground = cold
                .load_latest_model_context(LoadThreadHistoryParams {
                    thread_id: id,
                    include_archived: false,
                })
                .await
                .expect("foreground model context remains readable without receipt proof");
            assert_eq!(foreground.thread_id, id);
            assert!(
                foreground.items.iter().any(
                    |item| matches!(item, RolloutItem::SessionMeta(meta) if meta.meta.id == id)
                )
            );
            cold.shutdown_thread(id).await.unwrap();
        }
    }
}

#[tokio::test]
async fn canonical_attention_migration_preserves_lifetime_independent_of_rollback_and_bounded_context()
 {
    use codex_protocol::attention::ThreadAttentionDelivery;
    use codex_protocol::protocol::{ThreadRolledBackEvent, TurnStartedEvent, UserMessageEvent};
    for bounded_subagent in [false, true] {
        for uncertain in [false, true] {
            for proof in ["v1", "append-only", "unknown", "damaged"] {
                let home = TempDir::new().unwrap();
                let config = test_config(home.path());
                let db = codex_state::StateRuntime::init(
                    config.sqlite.clone(),
                    config.default_model_provider_id.clone(),
                )
                .await
                .unwrap();
                let store = LocalThreadStore::new(config.clone(), Some(db.clone()));
                let id = ThreadId::new();
                let mut params = create_thread_params(id);
                params.history_mode = ThreadHistoryMode::Legacy;
                if bounded_subagent {
                    params.source = codex_protocol::protocol::SessionSource::SubAgent(
                        codex_protocol::protocol::SubAgentSource::Other("receipt-fixture".into()),
                    );
                }
                store.create_thread(params).await.unwrap();
                let RolloutItem::EventMsg(EventMsg::ThreadAttention(mut held)) = notice(id) else {
                    unreachable!()
                };
                held.batch_id = "z-held".into();
                held.admission = ThreadAttentionResponse::Held {
                    reason: ThreadAttentionHeldReason::BlockingRequest,
                };
                let mut admitted = held.clone();
                admitted.batch_id = "a-admitted".into();
                admitted.admission = if uncertain {
                    ThreadAttentionResponse::Held {
                        reason: ThreadAttentionHeldReason::AdmissionUncertain,
                    }
                } else {
                    ThreadAttentionResponse::Accepted {
                        delivery: ThreadAttentionDelivery::Started,
                        turn_id: Some("removed".into()),
                    }
                };
                let expected = vec![held.clone(), admitted.clone()];
                let start = RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
                    turn_id: "removed".into(),
                    root_turn_id: None,
                    trace_id: None,
                    started_at: Some(10),
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                }));
                store
                    .append_items(AppendThreadItemsParams {
                        thread_id: id,
                        items: vec![
                            RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
                                message: "keep".into(),
                                ..Default::default()
                            })),
                            start,
                            RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
                                message: "remove".into(),
                                ..Default::default()
                            })),
                            RolloutItem::EventMsg(EventMsg::ThreadAttention(held)),
                            RolloutItem::EventMsg(EventMsg::ThreadAttention(admitted)),
                            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
                                ThreadRolledBackEvent { num_turns: 1 },
                            )),
                        ],
                    })
                    .await
                    .unwrap();
                if bounded_subagent {
                    let compacted = serde_json::from_value(serde_json::json!({
                        "message": "", "replacement_history": [], "window_number": 1,
                    }))
                    .unwrap();
                    store
                        .append_items(AppendThreadItemsParams {
                            thread_id: id,
                            items: vec![RolloutItem::Compacted(compacted)],
                        })
                        .await
                        .unwrap();
                }
                let path = store.live_rollout_path(id).await.unwrap();
                store.shutdown_thread(id).await.unwrap();
                codex_rollout::state_db::reconcile_rollout(
                    Some(db.as_ref()),
                    &path,
                    "test-provider",
                    None,
                    &[],
                    Some(false),
                    None,
                )
                .await;
                if proof != "v1" {
                    let bytes = std::fs::read_to_string(&path).unwrap();
                    let (head, rest) = bytes.split_once('\n').unwrap();
                    let mut head: serde_json::Value = serde_json::from_str(head).unwrap();
                    if proof == "unknown" {
                        head["payload"]["attention_receipt_lifetime_version"] = 99.into();
                    } else {
                        head["payload"]
                            .as_object_mut()
                            .unwrap()
                            .remove("attention_receipt_lifetime_version");
                    }
                    let suffix = if proof == "damaged" {
                        "{invalid}\n"
                    } else {
                        ""
                    };
                    std::fs::write(&path, format!("{head}\n{rest}{suffix}")).unwrap();
                }
                let report = store
                    .migrate_rollouts(super::super::RolloutMigrationOptions {
                        mode: super::super::RolloutMigrationMode::Apply,
                        thread_ids: vec![id],
                        max_mib_per_second: None,
                    })
                    .await
                    .unwrap();
                assert_eq!(report.outcomes.len(), 1);
                assert_eq!(
                    report.outcomes[0].status,
                    super::super::RolloutMigrationStatus::Migrated
                );
                let path = report.outcomes[0].rollout_path.clone();
                let proved = matches!(proof, "v1" | "append-only");
                assert_eq!(
                    codex_rollout::read_session_meta_line(&path)
                        .await
                        .unwrap()
                        .meta
                        .attention_receipt_lifetime_version,
                    proved.then_some(1)
                );
                let cold = LocalThreadStore::new(config, Some(db));
                cold.resume_thread(ResumeThreadParams {
                    thread_id: id,
                    rollout_path: Some(path),
                    history: None,
                    history_revision: None,
                    include_archived: false,
                    metadata: thread_metadata(),
                })
                .await
                .unwrap();
                let recovered = cold.load_canonical_attention_receipts(id).await;
                if proved {
                    let reduced =
                        crate::reduce_attention_receipts(id, recovered.unwrap().notices).unwrap();
                    assert_eq!(
                        crate::attention_receipts_for_preservation(reduced),
                        expected
                    );
                } else {
                    assert!(recovered.is_err());
                }
                let foreground = cold
                    .load_latest_model_context(LoadThreadHistoryParams {
                        thread_id: id,
                        include_archived: false,
                    })
                    .await
                    .unwrap();
                assert_eq!(foreground.thread_id, id);
                assert!(foreground.items.iter().any(
                    |item| matches!(item, RolloutItem::SessionMeta(meta) if meta.meta.id == id)
                ));
                if !bounded_subagent {
                    assert!(!foreground.items.iter().any(|item|
                        matches!(item, RolloutItem::EventMsg(EventMsg::UserMessage(event)) if event.message == "remove")));
                }
                cold.shutdown_thread(id).await.unwrap();
            }
        }
    }
}
