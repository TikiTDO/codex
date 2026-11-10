//! Native presentation witnesses; receipt lifetime is a separate assertion.
use super::super::test_support::test_config;
use super::*;
use crate::AppendThreadItemsParams;
use crate::CreateThreadParams;
use crate::PersistContext;
use crate::ThreadPersistenceMetadata;
use crate::ThreadStore;
use codex_protocol::items::TurnItem;
use codex_protocol::items::UserMessageItem;
use codex_protocol::models::BaseInstructions;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnStartedEvent;

async fn create(
    mode: ThreadHistoryMode,
    hidden: Option<u64>,
) -> (tempfile::TempDir, LocalThreadStore, ThreadId) {
    let home = tempfile::tempdir().unwrap();
    let config = test_config(home.path());
    let db = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .unwrap();
    let id = ThreadId::new();
    let store = LocalThreadStore::new(config, Some(db));
    seed_metadata(&store, id, mode).await;
    store
        .create_thread(params(home.path(), id, mode, hidden))
        .await
        .unwrap();
    store
        .persist_thread(id, PersistContext::Standard)
        .await
        .unwrap();
    (home, store, id)
}

fn params(
    home: &std::path::Path,
    id: ThreadId,
    mode: ThreadHistoryMode,
    hidden: Option<u64>,
) -> CreateThreadParams {
    CreateThreadParams {
        attached_thread: false,
        creator_user_id: None,
        creator_account_id: None,
        session_id: id.into(),
        thread_id: id,
        extra_config: None,
        forked_from_id: None,
        parent_thread_id: None,
        source: SessionSource::Exec,
        thread_source: None,
        originator: "presentation-fixture".into(),
        base_instructions: BaseInstructions::default(),
        dynamic_tools: Vec::new(),
        selected_capability_roots: Vec::new(),
        multi_agent_version: None,
        history_mode: mode,
        history_base: None,
        subagent_history_start_ordinal: hidden,
        initial_window_id: "window-1".into(),
        runtime_workspace_roots: None,
        metadata: ThreadPersistenceMetadata {
            cwd: Some(home.to_path_buf()),
            model_provider: "test-provider".into(),
            memory_mode: ThreadMemoryMode::Enabled,
        },
    }
}

async fn seed_metadata(store: &LocalThreadStore, id: ThreadId, mode: ThreadHistoryMode) {
    // Match native public paginated-read setup; a projection row is not state metadata.
    let mut metadata = codex_state::ThreadMetadataBuilder::new(
        id,
        store.config.codex_home.join("not-created-yet.jsonl"),
        chrono::Utc::now(),
        SessionSource::Exec,
    );
    metadata.history_mode = mode;
    store
        .state_db()
        .await
        .unwrap()
        .upsert_thread(&metadata.build(store.config.default_model_provider_id.as_str()))
        .await
        .unwrap();
}

async fn create_child(
    store: &LocalThreadStore,
    parent: ThreadId,
    base: Option<HistoryPosition>,
    attached: bool,
) -> ThreadId {
    let child = ThreadId::new();
    seed_metadata(store, child, ThreadHistoryMode::Paginated).await;
    let mut params = params(
        &store.config.codex_home,
        child,
        ThreadHistoryMode::Paginated,
        None,
    );
    params.forked_from_id = Some(parent);
    params.history_base = base;
    params.attached_thread = attached;
    store.create_thread(params).await.unwrap();
    store
        .persist_thread(child, PersistContext::Standard)
        .await
        .unwrap();
    child
}

fn start(id: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
        turn_id: id.into(),
        turn_attribution: None,
        root_turn_id: None,
        trace_id: None,
        started_at: Some(10),
        model_context_window: None,
        collaboration_mode_kind: Default::default(),
    }))
}
fn complete(id: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
        turn_id: id.into(),
        root_turn_id: None,
        last_agent_message: None,
        error: None,
        started_at: Some(10),
        completed_at: Some(20),
        duration_ms: Some(10_000),
        time_to_first_token_ms: None,
    }))
}
fn marker(id: ThreadId, turn: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::AttentionTurnStarted(
        codex_protocol::attention::AttentionTurnStartedNotification {
            thread_id: id.to_string(),
            turn_id: turn.into(),
        },
    ))
}
fn user(id: ThreadId, turn: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
        thread_id: id,
        turn_id: turn.into(),
        started_at_ms: None,
        completed_at_ms: 20_000,
        item: TurnItem::UserMessage(UserMessageItem {
            id: format!("user-{turn}"),
            client_id: None,
            content: Vec::new(),
        }),
    }))
}
async fn append(store: &LocalThreadStore, id: ThreadId, items: Vec<RolloutItem>) {
    store
        .append_items(AppendThreadItemsParams {
            thread_id: id,
            items,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn canonical_attention_presentation_early_and_post_terminal_users_are_not_summary_absence() {
    for early in [true, false] {
        let (_home, store, id) = create(ThreadHistoryMode::Paginated, None).await;
        let lifecycle = vec![start("t"), complete("t")];
        let mut items = vec![marker(id, "t")];
        if early {
            items.push(user(id, "t"));
        }
        items.extend(lifecycle);
        if !early {
            items.push(user(id, "t"));
        }
        append(&store, id, items).await;
        let view = load_presentation(&store, id).await.unwrap();
        assert!(view.retained_turn_ids.contains("t"));
        assert!(view.foreground_turn_ids.contains("t"));
        assert_eq!(view.markers.len(), 1);
        assert_eq!(view.scope[0].history_mode, ThreadHistoryMode::Paginated);
        let summary = store
            .list_turns(crate::ListTurnsParams {
                thread_id: id,
                include_archived: false,
                cursor: None,
                page_size: 10,
                sort_direction: crate::SortDirection::Asc,
                items_view: crate::StoredTurnItemsView::Summary,
            })
            .await
            .unwrap();
        assert_eq!(
            summary.turns[0].items.is_empty(),
            !early,
            "post-terminal first user is deliberately not a Summary oracle"
        );
        store.shutdown_thread(id).await.unwrap();
    }
}

#[tokio::test]
async fn canonical_attention_presentation_hidden_same_id_and_copied_mode_are_native_scoped() {
    let (_home, store, id) = create(ThreadHistoryMode::Paginated, Some(3)).await;
    append(
        &store,
        id,
        vec![
            user(id, "t"),
            start("t"),
            marker(id, "t"),
            start("t"),
            RolloutItem::SessionMeta(Box::new(SessionMetaLine {
                meta: SessionMeta {
                    id: ThreadId::new(),
                    history_mode: ThreadHistoryMode::Legacy,
                    ..Default::default()
                },
                git: None,
            })),
        ],
    )
    .await;
    let view = load_presentation(&store, id).await.unwrap();
    assert!(view.retained_turn_ids.contains("t"));
    assert!(!view.foreground_turn_ids.contains("t"));
    assert_eq!(view.markers.len(), 1);
    assert_eq!(view.scope[0].start_ordinal, 3);
    assert_eq!(view.scope[0].history_mode, ThreadHistoryMode::Paginated);
    assert_eq!(
        view.coverage[0].start_ordinal, 1,
        "physical integrity starts at the header base"
    );
    store.shutdown_thread(id).await.unwrap();
}

#[tokio::test]
async fn canonical_attention_presentation_bound_cut_excludes_later_native_turn_and_pages_all_ids() {
    let (_home, store, id) = create(ThreadHistoryMode::Paginated, None).await;
    let mut items = vec![marker(id, "t")];
    for n in 0..260 {
        items.push(start(&format!("t{n}")));
    }
    append(&store, id, items).await;
    let (recorder, _, _) = live_writer::live_writer_parts(&store, id).await.unwrap();
    let path = recorder.rollout_path().to_path_buf();
    let physical = store
        .resolve_attention_lineage(id, path.clone())
        .await
        .unwrap();
    let scope = store
        .resolve_attention_presentation_lineage(id, path)
        .await
        .unwrap();
    let handles = capture_handles(physical, || false).unwrap();
    let scan = scan_source(id, handles, false, Some(scope.clone())).unwrap();
    let view = scan.presentation.unwrap();
    let bound = bind_presentation_scope(scope, &view).unwrap();
    append(&store, id, vec![start("after-cut")]).await;
    let positive = positive_turn_ids(&store, id, &bound).await.unwrap();
    assert_eq!(positive.len(), 260);
    assert!(!positive.contains("after-cut"));
    assert!(
        !positive.contains("t"),
        "marker must never fabricate a turn"
    );
    store.shutdown_thread(id).await.unwrap();
}

#[test]
fn canonical_attention_presentation_intact_unmarked_legacy_is_not_receipt_readiness() {
    let id = ThreadId::new();
    let items = vec![
        RolloutItem::SessionMeta(Box::new(SessionMetaLine {
            meta: SessionMeta {
                id,
                ..Default::default()
            },
            git: None,
        })),
        marker(id, "t"),
        start("t"),
    ];
    let mut bytes = Vec::new();
    for item in items {
        serde_json::to_writer(
            &mut bytes,
            &codex_rollout::RolloutLine {
                timestamp: "2026-11-09T00:00:00Z".into(),
                ordinal: None,
                item,
            },
        )
        .unwrap();
        bytes.push(b'\n');
    }
    let segment = RolloutLineageSegment {
        rollout_id: id,
        owner_thread_id: id,
        rollout_path: "not-reopened".into(),
        start_ordinal: 1,
        end: None,
    };
    assert!(
        scan(
            id,
            vec![(
                segment.clone(),
                std::io::Cursor::new(&bytes),
                bytes.len() as u64
            )]
        )
        .is_err()
    );
    let result = scan_source(
        id,
        vec![(
            segment.clone(),
            std::io::Cursor::new(&bytes),
            bytes.len() as u64,
        )],
        false,
        Some(RolloutLineage {
            segments: vec![segment],
        }),
    )
    .unwrap()
    .presentation
    .unwrap();
    assert!(result.retained_turn_ids.contains("t"));
    assert_eq!(result.markers.len(), 1);
}

#[tokio::test]
async fn canonical_attention_presentation_native_owner_arc_survives_live_close() {
    let (home, store, id) = create(ThreadHistoryMode::Paginated, None).await;
    append(&store, id, vec![start("t"), complete("t")]).await;
    let path = store.live_rollout_path(id).await.unwrap();
    let lease = store.live_writer_locks.reserve_lifecycle(id).await;
    let owner = Arc::clone(
        &store
            .live_recorders
            .lock()
            .await
            .get(&id)
            .unwrap()
            .writer_lock,
    );
    let other = LocalThreadStore::new(test_config(home.path()), store.state_db().await);
    store.shutdown_thread(id).await.unwrap();
    assert!(matches!(
        other.acquire_writer_lock(id),
        Err(ThreadStoreError::Conflict { .. })
    ));
    assert!(matches!(
        other
            .revert_thread(crate::RevertThreadParams {
                thread_id: id,
                before_turn_id: "t".into(),
                multi_agent_version: None,
            })
            .await,
        Err(ThreadStoreError::Conflict { .. })
    ));
    assert!(matches!(
        other
            .archive_thread(crate::ArchiveThreadParams { thread_id: id })
            .await,
        Err(ThreadStoreError::Conflict { .. })
    ));
    assert_eq!(
        other
            .state_db()
            .await
            .unwrap()
            .get_thread(id)
            .await
            .unwrap()
            .unwrap()
            .rollout_path,
        path
    );
    assert!(
        store
            .live_writer_locks
            .coordination(id)
            .await
            .lifecycle
            .try_write()
            .is_err()
    );
    drop(lease);
    drop(owner);
    other
        .revert_thread(crate::RevertThreadParams {
            thread_id: id,
            before_turn_id: "t".into(),
            multi_agent_version: None,
        })
        .await
        .unwrap();
    assert_ne!(
        other
            .state_db()
            .await
            .unwrap()
            .get_thread(id)
            .await
            .unwrap()
            .unwrap()
            .rollout_path,
        path,
        "the same valid native replacement completes after ownership releases"
    );
}

#[tokio::test]
async fn canonical_attention_presentation_compaction_is_not_model_context_cut() {
    let (_home, store, id) = create(ThreadHistoryMode::Paginated, None).await;
    append(
        &store,
        id,
        vec![
            marker(id, "bg"),
            start("bg"),
            complete("bg"),
            RolloutItem::Compacted(
                serde_json::from_value(serde_json::json!({
                    "message": "", "replacement_history": [], "window_number": 1,
                }))
                .unwrap(),
            ),
        ],
    )
    .await;
    let view = load_presentation(&store, id).await.unwrap();
    assert!(view.retained_turn_ids.contains("bg"));
    assert!(!view.foreground_turn_ids.contains("bg"));
    assert_eq!(view.markers.len(), 1);
    store.shutdown_thread(id).await.unwrap();
}

#[tokio::test]
async fn canonical_attention_presentation_native_legacy_migration_retains_background_across_rollback()
 {
    let (_home, store, id) = create(ThreadHistoryMode::Legacy, None).await;
    append(
        &store,
        id,
        vec![
            marker(id, "bg"),
            start("bg"),
            complete("bg"),
            start("keep-user"),
            RolloutItem::EventMsg(EventMsg::UserMessage(
                codex_protocol::protocol::UserMessageEvent {
                    message: "keep".into(),
                    ..Default::default()
                },
            )),
            complete("keep-user"),
            start("remove-user"),
            RolloutItem::EventMsg(EventMsg::UserMessage(
                codex_protocol::protocol::UserMessageEvent {
                    message: "remove".into(),
                    ..Default::default()
                },
            )),
            complete("remove-user"),
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(
                codex_protocol::protocol::ThreadRolledBackEvent { num_turns: 1 },
            )),
        ],
    )
    .await;
    let legacy = load_presentation(&store, id).await.unwrap();
    assert!(legacy.retained_turn_ids.contains("bg"));
    assert!(!legacy.foreground_turn_ids.contains("bg"));
    assert_eq!(legacy.markers.len(), 1);
    store.shutdown_thread(id).await.unwrap();
    let report = store
        .migrate_rollouts(super::super::RolloutMigrationOptions {
            mode: super::super::RolloutMigrationMode::Apply,
            thread_ids: vec![id],
            max_mib_per_second: None,
        })
        .await
        .unwrap();
    assert_eq!(
        report.outcomes[0].status,
        super::super::RolloutMigrationStatus::Migrated
    );
    store
        .resume_thread(crate::ResumeThreadParams {
            thread_id: id,
            rollout_path: Some(report.outcomes[0].rollout_path.clone()),
            history: None,
            history_revision: None,
            include_archived: false,
            metadata: ThreadPersistenceMetadata {
                cwd: Some(store.config.codex_home.clone()),
                model_provider: "test-provider".into(),
                memory_mode: ThreadMemoryMode::Enabled,
            },
        })
        .await
        .unwrap();
    let migrated = load_presentation(&store, id).await.unwrap();
    assert!(migrated.retained_turn_ids.contains("bg"));
    assert!(!migrated.foreground_turn_ids.contains("bg"));
    assert!(migrated.markers.iter().any(|marker| marker.turn_id == "bg"));
    assert!(!migrated.retained_turn_ids.contains("remove-user"));
    store.shutdown_thread(id).await.unwrap();
}

#[tokio::test]
async fn canonical_attention_presentation_revert_keeps_lifetime_not_removed_turns_or_receipt_ids() {
    use codex_protocol::attention::ThreadAttentionHeldReason;
    use codex_protocol::attention::ThreadAttentionNotification;
    use codex_protocol::attention::ThreadAttentionOrigin;
    use codex_protocol::attention::ThreadAttentionOriginKind;
    use codex_protocol::attention::ThreadAttentionResponse;
    let (_home, store, id) = create(ThreadHistoryMode::Paginated, None).await;
    let uncertain = ThreadAttentionNotification {
        thread_id: id.to_string(),
        batch_id: "presentation-uncertain".into(),
        origins: vec![ThreadAttentionOrigin {
            kind: ThreadAttentionOriginKind::Peer,
            source_ref: "fixture:uncertain".into(),
            sender: None,
        }],
        admission: ThreadAttentionResponse::Held {
            reason: ThreadAttentionHeldReason::AdmissionUncertain,
        },
    };
    append(
        &store,
        id,
        vec![
            marker(id, "one"),
            start("one"),
            complete("one"),
            // Before the removed TurnStarted: this marker remains in the retained prefix.
            marker(id, "two"),
            start("two"),
            complete("two"),
            RolloutItem::EventMsg(EventMsg::ThreadAttention(uncertain.clone())),
        ],
    )
    .await;
    let initial = load_presentation(&store, id).await.unwrap();
    assert!(initial.retained_turn_ids.contains("one") && initial.retained_turn_ids.contains("two"));
    store.shutdown_thread(id).await.unwrap();
    for (before, expected) in [("two", Some("one")), ("one", None)] {
        store
            .revert_thread(crate::RevertThreadParams {
                thread_id: id,
                before_turn_id: before.into(),
                multi_agent_version: None,
            })
            .await
            .unwrap();
        let path = store
            .state_db()
            .await
            .unwrap()
            .get_thread(id)
            .await
            .unwrap()
            .unwrap()
            .rollout_path;
        store
            .resume_thread(crate::ResumeThreadParams {
                thread_id: id,
                rollout_path: Some(path),
                history: None,
                history_revision: None,
                include_archived: false,
                metadata: ThreadPersistenceMetadata {
                    cwd: Some(store.config.codex_home.clone()),
                    model_provider: "test-provider".into(),
                    memory_mode: ThreadMemoryMode::Enabled,
                },
            })
            .await
            .unwrap();
        let view = load_presentation(&store, id).await.unwrap();
        assert_eq!(
            view.retained_turn_ids,
            expected.into_iter().map(str::to_owned).collect()
        );
        assert!(
            view.markers.iter().any(|marker| marker.turn_id == before),
            "retained pre-start marker exercises the no-fabricated-turn rule"
        );
        let receipts = load(&store, id).await.unwrap();
        let reduced = crate::reduce_attention_receipts(id, receipts.notices).unwrap();
        assert_eq!(reduced.get(&uncertain.batch_id), Some(&uncertain));
        assert!(!view.retained_turn_ids.contains(&uncertain.batch_id));
        store.shutdown_thread(id).await.unwrap();
    }
}

#[test]
fn canonical_attention_presentation_strict_identity_ordinal_and_truncation_refuse() {
    let id = ThreadId::new();
    let mut valid = super::tests::bytes(id);
    let segment = RolloutLineageSegment {
        rollout_id: id,
        owner_thread_id: id,
        rollout_path: "not-reopened".into(),
        start_ordinal: 1,
        end: None,
    };
    let check = |bytes: Vec<u8>, physical: RolloutLineageSegment| {
        let scope = RolloutLineage {
            segments: vec![physical.clone()],
        };
        scan_source(
            id,
            vec![(
                physical,
                std::io::Cursor::new(bytes.clone()),
                bytes.len() as u64,
            )],
            false,
            Some(scope),
        )
    };
    let mut wrong = segment.clone();
    wrong.owner_thread_id = ThreadId::new();
    assert!(check(valid.clone(), wrong).is_err());
    valid.pop();
    assert!(check(valid, segment.clone()).is_err());
    let mut head: serde_json::Value = serde_json::from_slice(&super::tests::bytes(id)).unwrap();
    head["payload"]["history_mode"] = "paginated".into();
    head["ordinal"] = 4.into();
    let mut bad = serde_json::to_vec(&head).unwrap();
    bad.push(b'\n');
    assert!(check(bad, segment).is_err());
}

#[tokio::test]
async fn canonical_attention_presentation_acquisition_gap_replacement_is_not_old_generation_success()
 {
    let (_home, store, id) = create(ThreadHistoryMode::Paginated, None).await;
    append(&store, id, vec![marker(id, "t"), start("t"), complete("t")]).await;
    assert!(
        load_presentation(&store, id)
            .await
            .unwrap()
            .retained_turn_ids
            .contains("t"),
        "unchanged acquired generation is a real positive control"
    );
    let old_path = store.live_rollout_path(id).await.unwrap();
    let (entered, entry) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let reading = store.clone();
    let work = tokio::spawn(async move {
        load_source(&reading, id, true, Some((entered, released)))
            .await
            .map(|scan| scan.presentation.unwrap())
    });
    entry.await.unwrap();
    store.shutdown_thread(id).await.unwrap();
    store
        .revert_thread(crate::RevertThreadParams {
            thread_id: id,
            before_turn_id: "t".into(),
            multi_agent_version: None,
        })
        .await
        .unwrap();
    let path = store
        .state_db()
        .await
        .unwrap()
        .get_thread(id)
        .await
        .unwrap()
        .unwrap()
        .rollout_path;
    assert_ne!(path, old_path);
    assert!(
        old_path.exists(),
        "native revert deliberately retains the old source"
    );
    store
        .resume_thread(crate::ResumeThreadParams {
            thread_id: id,
            rollout_path: Some(path),
            history: None,
            history_revision: None,
            include_archived: false,
            metadata: ThreadPersistenceMetadata {
                cwd: Some(store.config.codex_home.clone()),
                model_provider: "test-provider".into(),
                memory_mode: ThreadMemoryMode::Enabled,
            },
        })
        .await
        .unwrap();
    release.send(()).unwrap();
    assert!(
        matches!(work.await.unwrap(), Err(ThreadStoreError::Conflict { .. })),
        "a successful old-generation A result would falsely restore removed t"
    );
    let current = load_presentation(&store, id).await.unwrap();
    assert!(current.retained_turn_ids.is_empty());
    assert!(
        current.markers.iter().any(|marker| marker.turn_id == "t"),
        "B retains the pre-start marker, but no receipt/marker may invent t"
    );
    store.shutdown_thread(id).await.unwrap();
}

#[tokio::test]
async fn canonical_attention_presentation_attached_scope_excludes_ancestor_same_id_user_and_bootstrap()
 {
    let (_home, store, parent) = create(ThreadHistoryMode::Paginated, None).await;
    append(
        &store,
        parent,
        vec![
            user(parent, "t"),
            start("t"),
            complete("t"),
            start("parent-running"),
        ],
    )
    .await;
    let prepared = store
        .prepare_fork(crate::PrepareForkParams {
            thread_id: parent,
            boundary: crate::ForkBoundary::Latest,
        })
        .await
        .unwrap();
    let child = create_child(&store, parent, prepared.history_base, true).await;
    drop(prepared);
    append(
        &store,
        child,
        vec![
            RolloutItem::EventMsg(EventMsg::TurnAborted(
                codex_protocol::protocol::TurnAbortedEvent {
                    root_turn_id: None,
                    turn_id: Some("parent-running".into()),
                    reason: codex_protocol::protocol::TurnAbortReason::Interrupted,
                    error: None,
                    started_at: Some(10),
                    completed_at: Some(20),
                    duration_ms: Some(10_000),
                },
            )),
            marker(child, "t"),
            start("t"),
            complete("t"),
        ],
    )
    .await;
    let view = load_presentation(&store, child).await.unwrap();
    assert_eq!(
        view.coverage.len(),
        2,
        "physical proof still covers the ancestor"
    );
    assert_eq!(
        view.scope.len(),
        1,
        "actual native attached HistoryPage stops at its bookmark"
    );
    assert_eq!(view.scope[0].owner_thread_id, child);
    assert!(
        view.scope[0].start_ordinal > view.coverage.last().unwrap().start_ordinal,
        "native visible start advances after bootstrap parent-abort, not physical header"
    );
    assert!(view.retained_turn_ids.contains("t"));
    assert!(
        !view.foreground_turn_ids.contains("t"),
        "invisible same-ID ancestor user is not child foreground"
    );
    assert!(!view.retained_turn_ids.contains("parent-running"));
    assert_eq!(view.markers.len(), 1);
    assert_eq!(view.markers[0].thread_id, child.to_string());
    store.shutdown_thread(child).await.unwrap();
    store.shutdown_thread(parent).await.unwrap();
}

#[tokio::test]
async fn canonical_attention_presentation_external_live_ancestor_conflict_is_not_resume_or_receipt_failure()
 {
    let (_home, parent_store, parent) = create(ThreadHistoryMode::Paginated, None).await;
    append(
        &parent_store,
        parent,
        vec![start("parent-t"), complete("parent-t")],
    )
    .await;
    let prepared = parent_store
        .prepare_fork(crate::PrepareForkParams {
            thread_id: parent,
            boundary: crate::ForkBoundary::Latest,
        })
        .await
        .unwrap();
    let child_store =
        LocalThreadStore::new(parent_store.config.clone(), parent_store.state_db().await);
    let child = create_child(&child_store, parent, prepared.history_base, false).await;
    drop(prepared);
    append(
        &child_store,
        child,
        vec![
            marker(child, "child-bg"),
            start("child-bg"),
            complete("child-bg"),
        ],
    )
    .await;
    assert!(
        matches!(
            load_presentation(&child_store, child).await,
            Err(ThreadStoreError::Conflict { .. })
        ),
        "another store's live ancestor refuses complete native ownership explicitly"
    );
    let ordinary = child_store
        .list_turns(crate::ListTurnsParams {
            thread_id: child,
            include_archived: false,
            cursor: None,
            page_size: 10,
            sort_direction: crate::SortDirection::Asc,
            items_view: crate::StoredTurnItemsView::NotLoaded,
        })
        .await
        .unwrap();
    assert!(ordinary.turns.iter().any(|turn| turn.turn_id == "child-bg"));
    assert!(
        load(&child_store, child).await.unwrap().notices.is_empty(),
        "independent strict receipt attempt remains available"
    );
    parent_store.shutdown_thread(parent).await.unwrap();
    let recovered = load_presentation(&child_store, child).await.unwrap();
    assert!(recovered.retained_turn_ids.contains("child-bg"));
    assert!(recovered.retained_turn_ids.contains("parent-t"));
    assert!(
        recovered
            .markers
            .iter()
            .any(|marker| marker.turn_id == "child-bg")
    );
    child_store.shutdown_thread(child).await.unwrap();
}

#[tokio::test]
async fn canonical_attention_presentation_ascending_native_pages_keep_initial_ids_under_shadow_and_backfill()
 {
    let (_home, store, parent) = create(ThreadHistoryMode::Paginated, None).await;
    let mut items = Vec::new();
    for n in 0..260 {
        let t = format!("t{n}");
        items.push(start(&t));
        if n == 2 {
            items.push(user(parent, &t));
        }
        items.push(complete(&t));
    }
    append(&store, parent, items).await;
    let prepared = store
        .prepare_fork(crate::PrepareForkParams {
            thread_id: parent,
            boundary: crate::ForkBoundary::Latest,
        })
        .await
        .unwrap();
    let child = create_child(&store, parent, prepared.history_base, false).await;
    drop(prepared);
    let mut items = vec![start("t1"), complete("t1"), start("t200"), complete("t200")];
    for n in 0..130 {
        items.push(start(&format!("c{n}")));
    }
    append(&store, child, items).await;
    let (_, parent_rollout, _) = live_writer::live_writer_parts(&store, parent)
        .await
        .unwrap();
    let (recorder, child_rollout, _) = live_writer::live_writer_parts(&store, child).await.unwrap();
    let path = recorder.rollout_path().to_path_buf();
    // Controlled projection lag: save actual native rows, not invented IDs/ordinals.
    // Deletion occurs BEFORE the read begins. During paging only insertion occurs,
    // matching the reviewed positive-membership contract; this is not producer traffic.
    let pool = store.thread_history_db().await.unwrap();
    sqlx::query("CREATE TABLE attention_delayed_rows AS SELECT * FROM thread_turns WHERE (thread_id = ? AND turn_id IN ('t1', 't200')) OR (thread_id = ? AND turn_id = 't2')")
        .bind(child_rollout.to_string()).bind(parent_rollout.to_string()).execute(pool).await.unwrap();
    sqlx::query("DELETE FROM thread_turns WHERE (thread_id = ? AND turn_id IN ('t1', 't200')) OR (thread_id = ? AND turn_id = 't2')")
        .bind(child_rollout.to_string()).bind(parent_rollout.to_string()).execute(pool).await.unwrap();
    let physical = store
        .resolve_attention_lineage(child, path.clone())
        .await
        .unwrap();
    let scope = store
        .resolve_attention_presentation_lineage(child, path)
        .await
        .unwrap();
    let scanned = scan_source(
        child,
        capture_handles(physical, || false).unwrap(),
        false,
        Some(scope.clone()),
    )
    .unwrap();
    let view = scanned.presentation.unwrap();
    assert!(
        view.foreground_turn_ids.contains("t2"),
        "missing projected turn never proves absent canonical user"
    );
    let bound = bind_presentation_scope(scope, &view).unwrap();
    let initial = positive_turn_ids(&store, child, &bound).await.unwrap();
    assert!(initial.contains("t1") && initial.contains("t200"));
    assert!(!initial.contains("t2"));
    let mut first = true;
    let mut cursor = None;
    let mut returned = std::collections::HashSet::new();
    loop {
        let page = super::super::thread_history::list_turns_on_lineage(
            &store,
            crate::ListTurnsParams {
                thread_id: child,
                include_archived: false,
                cursor,
                page_size: 128,
                sort_direction: crate::SortDirection::Asc,
                items_view: crate::StoredTurnItemsView::NotLoaded,
            },
            &bound,
        )
        .await
        .unwrap();
        returned.extend(page.turns.into_iter().map(|turn| turn.turn_id));
        if first {
            assert!(returned.contains("t1") && !returned.contains("t200"));
            sqlx::query("INSERT INTO thread_turns SELECT * FROM attention_delayed_rows")
                .execute(pool)
                .await
                .unwrap();
            first = false;
        }
        let Some(next) = page.next_cursor else {
            break;
        };
        cursor = Some(next);
    }
    assert!(
        initial.is_subset(&returned),
        "newer same-ID shadow can move IDs forward, not lose initial membership"
    );
    assert!(returned.contains("t200"));
    assert!(
        !returned.contains("t2"),
        "new behind-cursor backfill is explicitly not certified by this pass"
    );
    assert!(view.foreground_turn_ids.contains("t2"));
    store.shutdown_thread(child).await.unwrap();
    store.shutdown_thread(parent).await.unwrap();
}
