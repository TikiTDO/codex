//! Clear/wake admission at actual storage append and flush boundaries.
use super::*;
use codex_thread_store::*;
use pretty_assertions::assert_eq;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

struct ClearStore {
    inner: InMemoryThreadStore,
    block_flush: AtomicBool,
    fail_append: AtomicBool,
    fail_flush: AtomicBool,
    entered: Semaphore,
    release: Semaphore,
}
impl Default for ClearStore {
    fn default() -> Self {
        Self {
            inner: InMemoryThreadStore::default(),
            block_flush: AtomicBool::new(false),
            fail_append: AtomicBool::new(false),
            fail_flush: AtomicBool::new(false),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        }
    }
}
impl ThreadStore for ClearStore {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn append_items(&self, params: AppendThreadItemsParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            if self.fail_append.load(Ordering::SeqCst) {
                return Err(ThreadStoreError::Internal {
                    message: "fixture append failure".into(),
                });
            }
            self.inner.append_items(params).await
        })
    }
    fn flush_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            let fail_flush = self.fail_flush.load(Ordering::SeqCst);
            if self.block_flush.swap(false, Ordering::SeqCst) {
                self.entered.add_permits(1);
                self.release.acquire().await.unwrap().forget();
            }
            if fail_flush {
                return Err(ThreadStoreError::Internal {
                    message: "fixture flush failure".into(),
                });
            }
            self.inner.flush_thread(thread_id).await
        })
    }
    fn create_thread(&self, params: CreateThreadParams) -> ThreadStoreFuture<'_, ()> {
        self.inner.create_thread(params)
    }
    fn resume_thread(
        &self,
        params: ResumeThreadParams,
    ) -> ThreadStoreFuture<'_, Arc<Vec<RolloutItem>>> {
        self.inner.resume_thread(params)
    }
    fn persist_thread(
        &self,
        thread_id: ThreadId,
        context: PersistContext,
    ) -> ThreadStoreFuture<'_, ()> {
        self.inner.persist_thread(thread_id, context)
    }
    fn shutdown_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        self.inner.shutdown_thread(thread_id)
    }
    fn discard_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        self.inner.discard_thread(thread_id)
    }
    fn load_history(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreFuture<'_, StoredThreadHistory> {
        self.inner.load_history(params)
    }
    fn read_thread(&self, params: ReadThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        self.inner.read_thread(params)
    }
    fn read_thread_by_rollout_path(
        &self,
        params: ReadThreadByRolloutPathParams,
    ) -> ThreadStoreFuture<'_, StoredThread> {
        self.inner.read_thread_by_rollout_path(params)
    }
    fn list_threads(&self, params: ListThreadsParams) -> ThreadStoreFuture<'_, ThreadPage> {
        self.inner.list_threads(params)
    }
    fn update_thread_metadata(
        &self,
        params: UpdateThreadMetadataParams,
    ) -> ThreadStoreFuture<'_, Option<StoredThread>> {
        self.inner.update_thread_metadata(params)
    }
    fn archive_thread(&self, params: ArchiveThreadParams) -> ThreadStoreFuture<'_, ()> {
        self.inner.archive_thread(params)
    }
    fn unarchive_thread(&self, params: ArchiveThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        self.inner.unarchive_thread(params)
    }
    fn delete_thread(&self, params: DeleteThreadParams) -> ThreadStoreFuture<'_, ()> {
        self.inner.delete_thread(params)
    }
}

async fn queue_trigger(session: &Session) {
    let message = codex_protocol::protocol::InterAgentCommunication::new(
        codex_protocol::AgentPath::try_from("/root/worker").unwrap(),
        codex_protocol::AgentPath::root(),
        Vec::new(),
        "pending clear wake".into(),
        true,
    );
    session
        .input_queue
        .enqueue_mailbox_communication(message.into(), Default::default())
        .await;
}

#[tokio::test]
async fn clear_context_holds_admission_through_real_flush_then_pending_wake_uses_clear() {
    let (mut session, _) = make_session_and_context().await;
    let store = Arc::new(ClearStore::default());
    attach_test_thread_store(&mut session, store.clone()).await;
    queue_trigger(&session).await;
    store.block_flush.store(true, Ordering::SeqCst);
    let mut clear = Box::pin(tokio::task::unconstrained(
        session.clear_effective_context(),
    ));
    assert!(futures::poll!(clear.as_mut()).is_pending());
    store.entered.acquire().await.unwrap().forget();
    assert!(session.active_turn.try_lock().is_err());
    let mut reserve = Box::pin(tokio::task::unconstrained(
        session.reserve_pending_work_turn(),
    ));
    assert!(futures::poll!(reserve.as_mut()).is_pending());
    assert!(session.input_queue.has_trigger_turn_mailbox_items().await);
    store.release.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(5), clear)
        .await
        .unwrap()
        .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(5), reserve)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        format!(
            "{:?}",
            session
                .clone_history()
                .await
                .raw_items()
                .collect::<Vec<_>>()
        )
        .contains("Context was cleared")
    );
}

#[tokio::test]
async fn clear_context_wake_first_real_reservation_is_preserved() {
    let (mut session, _) = make_session_and_context().await;
    let store = Arc::new(ClearStore::default());
    attach_test_thread_store(&mut session, store.clone()).await;
    queue_trigger(&session).await;
    let (reserved, _) = session.reserve_pending_work_turn().await.unwrap();
    assert!(session.clear_effective_context().await.is_err());
    let active = session.active_turn.lock().await;
    assert!(Arc::ptr_eq(&active.as_ref().unwrap().turn_state, &reserved));
    assert!(session.input_queue.has_trigger_turn_mailbox_items().await);
}

#[tokio::test]
async fn clear_context_append_and_flush_failures_do_not_report_durable_success() {
    for append in [true, false] {
        let (mut session, _) = make_session_and_context().await;
        let store = Arc::new(ClearStore::default());
        attach_test_thread_store(&mut session, store.clone()).await;
        let old_history = session.clone_history().await.annotated_items().to_vec();
        let old_window = session.current_window_id().await;
        store.fail_append.store(append, Ordering::SeqCst);
        store.fail_flush.store(!append, Ordering::SeqCst);
        assert!(session.clear_effective_context().await.is_err());
        assert!(session.active_turn.lock().await.is_none());
        assert_eq!(
            session.clone_history().await.annotated_items().to_vec(),
            old_history
        );
        assert_eq!(session.current_window_id().await, old_window);
        // The append may have reached disk; live memory is unchanged, not rolled back.
        assert!(
            session
                .clone_history()
                .await
                .reference_context_item()
                .is_none()
        );
    }
}

#[tokio::test]
async fn clear_context_cancelled_during_real_flush_releases_admission_not_pending_mail() {
    let (mut session, _) = make_session_and_context().await;
    let store = Arc::new(ClearStore::default());
    attach_test_thread_store(&mut session, store.clone()).await;
    queue_trigger(&session).await;
    store.block_flush.store(true, Ordering::SeqCst);
    let mut clear = Box::pin(tokio::task::unconstrained(
        session.clear_effective_context(),
    ));
    assert!(futures::poll!(clear.as_mut()).is_pending());
    store.entered.acquire().await.unwrap().forget();
    drop(clear);
    assert!(session.active_turn.try_lock().unwrap().is_none());
    assert!(session.input_queue.has_trigger_turn_mailbox_items().await);
    assert!(session.reserve_pending_work_turn().await.is_some());
}

#[tokio::test]
async fn clear_context_refuses_during_actual_completion_flush_and_preserves_trigger_mail() {
    let (mut session, turn, _) = make_session_and_context_with_rx().await;
    let store = Arc::new(ClearStore::default());
    attach_test_thread_store(Arc::get_mut(&mut session).unwrap(), store.clone()).await;
    session
        .spawn_task(
            Arc::clone(&turn),
            Vec::new(),
            NeverEndingTask {
                kind: TaskKind::Regular,
                listen_to_cancellation_token: false,
            },
        )
        .await;
    queue_trigger(&session).await;
    store.block_flush.store(true, Ordering::SeqCst);
    let mut finish = Box::pin(tokio::task::unconstrained(
        session.on_task_finished(Arc::clone(&turn), Ok(None)),
    ));
    assert!(futures::poll!(finish.as_mut()).is_pending());
    tokio::time::timeout(std::time::Duration::from_secs(5), store.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert!(session.clear_effective_context().await.is_err());
    assert!(session.input_queue.has_trigger_turn_mailbox_items().await);
    store.release.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(5), finish)
        .await
        .unwrap();
    assert!(
        !format!(
            "{:?}",
            session
                .clone_history()
                .await
                .raw_items()
                .collect::<Vec<_>>()
        )
        .contains("Context was cleared")
    );
}

#[test_case::test_case(false, false; "plain_success")]
#[test_case::test_case(true, false; "realtime_success")]
#[test_case::test_case(false, true; "plain_flush_failure")]
#[test_case::test_case(true, true; "realtime_flush_failure")]
#[tokio::test]
async fn clear_context_private_completion_keeps_admission_through_actual_flush(
    realtime: bool,
    fail_flush: bool,
) {
    let (mut session, turn, events) = make_session_and_context_with_rx().await;
    let store = Arc::new(ClearStore::default());
    let mutable = Arc::get_mut(&mut session).unwrap();
    attach_test_thread_store(mutable, store.clone()).await;
    if realtime {
        mutable.realtime_history = Some(Mutex::new(
            crate::realtime_history::RealtimeHistoryState::default(),
        ));
    }
    session
        .state
        .lock()
        .await
        .session_configuration
        .trusted_guardian_reviewer = true;
    session
        .spawn_task(
            Arc::clone(&turn),
            Vec::new(),
            NeverEndingTask {
                kind: TaskKind::Regular,
                listen_to_cancellation_token: false,
            },
        )
        .await;
    session.emit_turn_started(&turn, TaskKind::Regular).await;
    while events.try_recv().is_ok() {}
    store.block_flush.store(true, Ordering::SeqCst);
    store.fail_flush.store(fail_flush, Ordering::SeqCst);
    let mut finish = Box::pin(tokio::task::unconstrained(
        session.on_task_finished(Arc::clone(&turn), Ok(None)),
    ));
    assert!(futures::poll!(finish.as_mut()).is_pending());
    store.entered.acquire().await.unwrap().forget();
    assert!(
        session.active_turn.try_lock().is_err(),
        "private persistence carries existing admission"
    );
    assert!(
        events.try_recv().is_err(),
        "terminal delivery must follow flush"
    );
    let mut clear = Box::pin(tokio::task::unconstrained(
        session.clear_effective_context(),
    ));
    assert!(futures::poll!(clear.as_mut()).is_pending());
    store.fail_flush.store(false, Ordering::SeqCst);
    store.release.add_permits(1);
    let ((), result) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(finish, clear)
    })
    .await
    .unwrap();
    result.unwrap();
    assert!(session.active_turn.lock().await.is_none());
    let mut completions = 0;
    while let Ok(event) = events.try_recv() {
        if matches!(event.msg, EventMsg::TurnComplete(_)) {
            completions += 1;
        }
    }
    assert_eq!(completions, 1);
    let history = store
        .load_history(LoadThreadHistoryParams {
            thread_id: session.thread_id,
            include_archived: false,
        })
        .await
        .unwrap();
    assert_eq!(
        history
            .items
            .iter()
            .filter(|item| matches!(item, RolloutItem::EventMsg(EventMsg::TurnComplete(_))))
            .count(),
        1
    );
}

#[tokio::test]
async fn clear_context_private_completion_releases_admission_before_blocked_parent_delivery() {
    let (mut session, turn, _) = make_session_and_context_with_rx().await;
    let store = Arc::new(ClearStore::default());
    let mutable = Arc::get_mut(&mut session).unwrap();
    attach_test_thread_store(mutable, store).await;
    let (sender, receiver) = async_channel::bounded(1);
    mutable.tx_event = sender;
    mutable.realtime_history = Some(Mutex::new(
        crate::realtime_history::RealtimeHistoryState::default(),
    ));
    session
        .state
        .lock()
        .await
        .session_configuration
        .trusted_guardian_reviewer = true;
    session
        .spawn_task(
            Arc::clone(&turn),
            Vec::new(),
            NeverEndingTask {
                kind: TaskKind::Regular,
                listen_to_cancellation_token: false,
            },
        )
        .await;
    session.emit_turn_started(&turn, TaskKind::Regular).await; // Fill the real delivery channel.
    let mut finish = Box::pin(tokio::task::unconstrained(
        session.on_task_finished(Arc::clone(&turn), Ok(None)),
    ));
    assert!(futures::poll!(finish.as_mut()).is_pending());
    assert!(
        session.active_turn.try_lock().unwrap().is_none(),
        "admission released before send can complete"
    );
    queue_trigger(&session).await;
    let (next_state, _) = session
        .reserve_pending_work_turn()
        .await
        .expect("parent may reuse reviewer immediately");
    assert!(matches!(
        receiver.recv().await.unwrap().msg,
        EventMsg::TurnStarted(_)
    ));
    tokio::time::timeout(std::time::Duration::from_secs(5), finish)
        .await
        .unwrap();
    assert!(matches!(
        receiver.recv().await.unwrap().msg,
        EventMsg::TurnComplete(_)
    ));
    assert!(Arc::ptr_eq(
        &session
            .active_turn
            .lock()
            .await
            .as_ref()
            .unwrap()
            .turn_state,
        &next_state
    ));
}

#[tokio::test]
async fn clear_context_private_cancelled_flush_releases_guard_but_not_finishing_owner_or_mail() {
    let (mut session, turn, _) = make_session_and_context_with_rx().await;
    let store = Arc::new(ClearStore::default());
    attach_test_thread_store(Arc::get_mut(&mut session).unwrap(), store.clone()).await;
    session
        .state
        .lock()
        .await
        .session_configuration
        .trusted_guardian_reviewer = true;
    session
        .spawn_task(
            Arc::clone(&turn),
            Vec::new(),
            NeverEndingTask {
                kind: TaskKind::Regular,
                listen_to_cancellation_token: false,
            },
        )
        .await;
    queue_trigger(&session).await;
    store.block_flush.store(true, Ordering::SeqCst);
    let mut finish = Box::pin(tokio::task::unconstrained(
        session.on_task_finished(turn, Ok(None)),
    ));
    assert!(futures::poll!(finish.as_mut()).is_pending());
    store.entered.acquire().await.unwrap().forget();
    drop(finish);
    assert!(
        session
            .active_turn
            .try_lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .finishing
            .is_some()
    );
    assert!(session.clear_effective_context().await.is_err());
    assert!(session.input_queue.has_trigger_turn_mailbox_items().await);
}

#[tokio::test]
async fn clear_context_ordinary_finisher_never_clears_replacement_after_terminal_flush() {
    let (mut session, turn, _) = make_session_and_context_with_rx().await;
    let store = Arc::new(ClearStore::default());
    attach_test_thread_store(Arc::get_mut(&mut session).unwrap(), store.clone()).await;
    session
        .spawn_task(
            Arc::clone(&turn),
            Vec::new(),
            NeverEndingTask {
                kind: TaskKind::Regular,
                listen_to_cancellation_token: false,
            },
        )
        .await;
    store.block_flush.store(true, Ordering::SeqCst);
    let mut finish = Box::pin(tokio::task::unconstrained(
        session.on_task_finished(turn, Ok(None)),
    ));
    assert!(futures::poll!(finish.as_mut()).is_pending());
    store.entered.acquire().await.unwrap().forget();
    let replacement = ActiveTurn::default();
    let replacement_state = Arc::clone(&replacement.turn_state);
    *session.active_turn.lock().await = Some(replacement);
    store.release.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(5), finish)
        .await
        .unwrap();
    assert!(Arc::ptr_eq(
        &session
            .active_turn
            .lock()
            .await
            .as_ref()
            .unwrap()
            .turn_state,
        &replacement_state
    ));
}

#[tokio::test]
async fn clear_context_real_ordinary_input_waits_for_barrier_then_starts_on_cleared_history() {
    let server = core_test_support::responses::start_mock_server().await;
    let requests = core_test_support::responses::mount_sse_once(
        &server,
        core_test_support::responses::sse(vec![
            core_test_support::responses::ev_assistant_message("after-clear", "done"),
            core_test_support::responses::ev_completed("after-clear-response"),
        ]),
    )
    .await;
    let uri = format!("{}/v1", server.uri());
    let (mut session, _, events) = make_session_and_context_with_auth_and_config_and_rx(
        CodexAuth::create_dummy_chatgpt_auth_for_testing(),
        Vec::new(),
        move |config| {
            config.model_provider.base_url = Some(uri);
            config.model_provider.supports_websockets = false;
        },
    )
    .await;
    let store = Arc::new(ClearStore::default());
    attach_test_thread_store(Arc::get_mut(&mut session).unwrap(), store.clone()).await;
    store.block_flush.store(true, Ordering::SeqCst);
    let mut clear = Box::pin(tokio::task::unconstrained(
        session.clear_effective_context(),
    ));
    assert!(futures::poll!(clear.as_mut()).is_pending());
    store.entered.acquire().await.unwrap().forget();
    let mut ordinary = Box::pin(tokio::task::unconstrained(
        super::super::turn_input::handle(
            &session,
            TurnInputRequest::user_input(vec![UserInput::Text {
                text: "ORDINARY_INPUT_AFTER_CLEAR".into(),
                text_elements: Vec::new(),
            }]),
            TurnInputMode::StartOrSteer,
            "ordinary-after-clear".into(),
        ),
    ));
    assert!(futures::poll!(ordinary.as_mut()).is_pending());
    assert!(requests.requests().is_empty());
    store.release.add_permits(1);
    clear.await.unwrap();
    let accepted = tokio::time::timeout(std::time::Duration::from_secs(5), ordinary)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(accepted, TurnInputSubmission::Started { .. }));
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !matches!(events.recv().await.unwrap().msg, EventMsg::TurnComplete(_)) {}
    })
    .await
    .unwrap();
    let body = requests.single_request().body_json().to_string();
    assert!(body.contains("Context was cleared"));
    assert!(body.contains("ORDINARY_INPUT_AFTER_CLEAR"));
}
