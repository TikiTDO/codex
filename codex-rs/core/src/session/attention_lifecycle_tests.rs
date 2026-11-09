//! Actual native suspension stages with disposable LocalThreadStore failure seams.
use super::*;
use crate::state::TaskKind;
use crate::tasks::{SessionTask, SessionTaskResult};
use codex_protocol::attention::{AttentionError, ThreadAttentionResponse};
use codex_thread_store::*;
use pretty_assertions::assert_eq;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{OnceLock, Weak};

struct SuspensionFailureStore {
    inner: Arc<dyn ThreadStore>,
    flush_calls: AtomicUsize,
    fail_flush_at: AtomicUsize,
    fail_late_flush: AtomicBool,
    session: OnceLock<Weak<Session>>,
    fail_close: AtomicBool,
    shutdown_calls: AtomicUsize,
    block_flush: AtomicBool,
    entered: Semaphore,
    release: Semaphore,
}
impl SuspensionFailureStore {
    fn new(inner: Arc<dyn ThreadStore>) -> Self {
        Self {
            inner,
            flush_calls: AtomicUsize::new(0),
            fail_flush_at: AtomicUsize::new(0),
            fail_late_flush: AtomicBool::new(false),
            session: OnceLock::new(),
            fail_close: AtomicBool::new(false),
            shutdown_calls: AtomicUsize::new(0),
            block_flush: AtomicBool::new(false),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        }
    }
    fn configure(&self, phase: FailurePhase) {
        self.flush_calls.store(0, Ordering::SeqCst);
        self.fail_flush_at.store(
            match phase {
                FailurePhase::InitialFlush => 1,
                FailurePhase::FinalFlush => 0,
                FailurePhase::WriterClose => 0,
            },
            Ordering::SeqCst,
        );
        self.fail_late_flush
            .store(matches!(phase, FailurePhase::FinalFlush), Ordering::SeqCst);
        self.fail_close
            .store(matches!(phase, FailurePhase::WriterClose), Ordering::SeqCst);
    }
}
impl ThreadStore for SuspensionFailureStore {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn append_items(&self, params: AppendThreadItemsParams) -> ThreadStoreFuture<'_, ()> {
        self.inner.append_items(params)
    }
    fn load_canonical_attention_receipts(
        &self,
        thread_id: ThreadId,
    ) -> ThreadStoreFuture<'_, CanonicalAttentionReceipts> {
        self.inner.load_canonical_attention_receipts(thread_id)
    }
    fn flush_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            let call = self.flush_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if self.block_flush.swap(false, Ordering::SeqCst) {
                self.entered.add_permits(1);
                self.release.acquire().await.unwrap().forget();
            }
            let late_failure = if self.fail_late_flush.load(Ordering::SeqCst) {
                self.session
                    .get()
                    .and_then(Weak::upgrade)
                    .expect("fixture retains its active session")
                    .state
                    .lock()
                    .await
                    .shutting_down
            } else {
                false
            };
            if call == self.fail_flush_at.load(Ordering::SeqCst) || late_failure {
                return Err(ThreadStoreError::Internal {
                    message: format!("fixture flush {call} failure"),
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
        Box::pin(async move {
            self.shutdown_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_close.load(Ordering::SeqCst) {
                return Err(ThreadStoreError::Internal {
                    message: "fixture writer-close failure".into(),
                });
            }
            self.inner.shutdown_thread(thread_id).await
        })
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

#[derive(Clone, Copy, Debug)]
enum FailurePhase {
    InitialFlush,
    FinalFlush,
    WriterClose,
}

struct WaitingTask;
impl SessionTask for WaitingTask {
    fn kind(&self) -> TaskKind {
        TaskKind::Regular
    }
    fn span_name(&self) -> &'static str {
        "session_task.attention_suspend_fixture"
    }
    async fn run(
        self: Arc<Self>,
        _session: Arc<Session>,
        _ctx: Arc<TurnContext>,
        _input: Vec<TurnInput>,
        cancellation: CancellationToken,
    ) -> SessionTaskResult {
        cancellation.cancelled().await;
        Ok(None)
    }
}

async fn running_fixture() -> (
    Arc<Session>,
    Arc<SuspensionFailureStore>,
    async_channel::Receiver<Event>,
) {
    let (mut session, context, events) = make_session_and_context_with_rx().await;
    let store = Arc::new(SuspensionFailureStore::new(Arc::clone(
        &session.services.thread_store,
    )));
    attach_test_thread_store(Arc::get_mut(&mut session).unwrap(), store.clone()).await;
    assert!(store.session.set(Arc::downgrade(&session)).is_ok());
    session.spawn_task(context, Vec::new(), WaitingTask).await;
    let (notice, request) = canonical_attention_request(&session, "prior", "fixture:prior");
    assert!(matches!(
        crate::attention::handle(&session, notice, request, false, "prior".into())
            .await
            .unwrap(),
        ThreadAttentionResponse::Accepted { .. }
    ));
    (session, store, events)
}

async fn fixture_rollout_path(session: &Session, store: &SuspensionFailureStore) -> PathBuf {
    // The proxy correctly has no public local-only visibility. Only this fixture
    // knows that the acquired inner persistence owner is a LocalThreadStore.
    assert!(session.current_rollout_path().await.unwrap().is_none());
    store
        .inner
        .as_any()
        .downcast_ref::<LocalThreadStore>()
        .expect("fixture acquired a native local store")
        .live_rollout_path(session.thread_id)
        .await
        .unwrap()
}

async fn reacquire_fixture_writer(
    session: &Session,
    store: &SuspensionFailureStore,
    path: PathBuf,
) -> LiveThread {
    store.fail_flush_at.store(0, Ordering::SeqCst);
    store.fail_late_flush.store(false, Ordering::SeqCst);
    store.fail_close.store(false, Ordering::SeqCst);
    match session.live_thread().unwrap().shutdown().await {
        Ok(()) => {}
        Err(ThreadStoreError::ThreadNotFound { thread_id }) if thread_id == session.thread_id => {}
        Err(error) => panic!("unexpected fixture writer cleanup failure: {error}"),
    }
    // Already-absent cleanup is not a fence. This fixture has one writer and no
    // child execution: require a real native acquisition, then strict recovery.
    let config = session.get_config().await;
    let (live, _) = LiveThread::resume(
        store.inner.clone(),
        ThreadHistoryMode::Legacy,
        ResumeThreadParams {
            thread_id: session.thread_id,
            rollout_path: Some(path),
            history: None,
            history_revision: None,
            include_archived: false,
            metadata: ThreadPersistenceMetadata {
                cwd: Some(config.cwd.to_path_buf()),
                model_provider: config.model_provider_id.clone(),
                memory_mode: ThreadMemoryMode::Enabled,
            },
        },
        None,
    )
    .await
    .expect("genuine fixture writer reacquisition");
    live
}

#[test_case::test_case(FailurePhase::InitialFlush; "initial_flush_preserves_task_and_ledger")]
#[test_case::test_case(FailurePhase::FinalFlush; "final_flush_keeps_closed_owner")]
#[test_case::test_case(FailurePhase::WriterClose; "writer_close_keeps_closed_owner")]
#[tokio::test]
async fn canonical_attention_suspension_failures_keep_native_stage_custody(phase: FailurePhase) {
    let (session, store, events) = running_fixture().await;
    let path = fixture_rollout_path(&session, &store).await;
    let cancellation = session
        .active_turn
        .lock()
        .await
        .as_ref()
        .unwrap()
        .task
        .as_ref()
        .unwrap()
        .cancellation_token
        .clone();
    store.configure(phase);
    let error =
        crate::session::turn_suspension::suspend_turn_and_shutdown(&session, "suspend".into())
            .await
            .unwrap_err();
    let message = error.to_string();
    let snapshot = session
        .services
        .attention_ledger
        .snapshot(session.thread_id, session.live_thread())
        .await;
    match phase {
        FailurePhase::InitialFlush => {
            assert!(message.contains("flush before"));
            assert!(!cancellation.is_cancelled());
            assert!(
                session
                    .active_turn
                    .lock()
                    .await
                    .as_ref()
                    .unwrap()
                    .task
                    .is_some()
            );
            assert!(!session.state.lock().await.shutting_down);
            let notices = snapshot.unwrap();
            assert_eq!(notices.len(), 1);
            assert!(matches!(
                notices[0].admission,
                ThreadAttentionResponse::Accepted { .. }
            ));
        }
        FailurePhase::FinalFlush | FailurePhase::WriterClose => {
            assert!(
                message.contains(if matches!(phase, FailurePhase::FinalFlush) {
                    "flush after"
                } else {
                    "close suspended"
                })
            );
            assert!(cancellation.is_cancelled());
            assert!(session.active_turn.lock().await.is_none());
            assert!(session.state.lock().await.shutting_down);
            assert!(matches!(snapshot, Err(AttentionError::Stopped)));
            let (notice, request) =
                canonical_attention_request(&session, "queued", "fixture:queued");
            assert!(matches!(
                crate::attention::handle(&session, notice, request, true, "queued".into()).await,
                Err(AttentionError::Stopped)
            ));
        }
    }
    // No phase reported successful handoff. A generic later event would not prove a fence.
    while let Ok(event) = events.try_recv() {
        assert!(!matches!(event.msg, EventMsg::ShutdownComplete));
    }
    store.fail_flush_at.store(0, Ordering::SeqCst);
    store.fail_late_flush.store(false, Ordering::SeqCst);
    store.fail_close.store(false, Ordering::SeqCst);
    crate::session::handlers::shutdown_session_runtime(&session).await;
    let live = reacquire_fixture_writer(&session, &store, path.clone()).await;
    let (items, _, errors) = RolloutRecorder::load_rollout_items(&path).await.unwrap();
    assert_eq!(errors, 0);
    let persisted = items
        .iter()
        .filter_map(|item| match item {
            RolloutItem::EventMsg(EventMsg::ThreadAttention(notice)) => Some(notice),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        persisted.len(),
        2,
        "queued Stopped request never writes an uncertain intent"
    );
    let cold = crate::attention::AttentionLedger::default()
        .snapshot(session.thread_id, Some(&live))
        .await
        .unwrap();
    assert_eq!(cold.len(), 1);
    assert_eq!(cold[0].batch_id, "prior");
    assert!(matches!(
        cold[0].admission,
        ThreadAttentionResponse::Accepted { .. }
    ));
    live.shutdown().await.unwrap();
}

#[tokio::test]
async fn canonical_attention_initial_suspension_failure_does_not_repair_unavailable_ledger() {
    let (session, store, _events) = running_fixture().await;
    store.configure(FailurePhase::InitialFlush);
    let (notice, request) = canonical_attention_request(&session, "uncertain", "fixture:uncertain");
    assert!(
        crate::attention::handle(&session, notice, request, false, "uncertain".into())
            .await
            .is_err()
    );
    assert!(matches!(
        session
            .services
            .attention_ledger
            .snapshot(session.thread_id, session.live_thread())
            .await,
        Err(AttentionError::Unavailable(_))
    ));
    store.configure(FailurePhase::InitialFlush);
    assert!(
        crate::session::turn_suspension::suspend_turn_and_shutdown(&session, "early".into())
            .await
            .is_err()
    );
    assert!(
        session
            .active_turn
            .lock()
            .await
            .as_ref()
            .unwrap()
            .task
            .is_some()
    );
    assert!(!session.state.lock().await.shutting_down);
    assert!(matches!(
        session
            .services
            .attention_ledger
            .snapshot(session.thread_id, session.live_thread())
            .await,
        Err(AttentionError::Unavailable(_))
    ));
    store.fail_flush_at.store(0, Ordering::SeqCst);
    crate::session::handlers::shutdown_session_runtime(&session).await;
    session.live_thread().unwrap().shutdown().await.unwrap();
}

#[test_case::test_case(FailurePhase::InitialFlush; "lost_waiter_before_teardown")]
#[test_case::test_case(FailurePhase::FinalFlush; "lost_waiter_after_teardown_flush")]
#[test_case::test_case(FailurePhase::WriterClose; "lost_waiter_after_teardown_close")]
#[tokio::test]
async fn canonical_attention_lost_suspend_waiter_keeps_queued_request_order(phase: FailurePhase) {
    let (session, store, events) = running_fixture().await;
    let path = fixture_rollout_path(&session, &store).await;
    store.configure(phase);
    let config = session.get_config().await;
    let (thread, receiver) = raw_fixture_thread(session.clone(), events).await;
    let mut raw =
        Box::pin(thread.inject_attention_if_running(raw_fixture_context("raw-after-suspend")));
    let (lost_reply, lost_receiver) = tokio::sync::oneshot::channel();
    let (reply, received) = tokio::sync::oneshot::channel();
    let (notice, request) = canonical_attention_request(&session, "queued", "fixture:queued");
    for (index, (id, op)) in [
        ("suspend", Op::SuspendTurnAndShutdown { reply: lost_reply }),
        (
            "queued",
            Op::Attention {
                notice,
                request: Box::new(request),
                blocking_request: false,
                reply,
            },
        ),
        ("shutdown", Op::Shutdown),
    ]
    .into_iter()
    .enumerate()
    {
        thread
            .io
            .tx_sub
            .send(crate::session::Submission {
                id: id.into(),
                op,
                turn_extension_init: None,
                trace: None,
                parent_turn_id: None,
                root_turn_id: None,
                residency_guard: None,
                message_delivery: None,
            })
            .await
            .unwrap();
        if index == 0 {
            assert!(
                futures::poll!(&mut raw).is_pending(),
                "public raw operation enqueued behind suspension"
            );
        }
    }
    drop(lost_receiver); // Core has enqueued custody; cancelling observation is not teardown cancellation.
    crate::session::handlers::submission_loop(session.clone(), config, receiver).await;
    let raw_result = raw.await.unwrap();
    if matches!(phase, FailurePhase::InitialFlush) {
        assert_eq!(raw_result.unwrap(), "turn_id");
    } else {
        assert_eq!(
            raw_result,
            Err(codex_protocol::attention::ThreadAttentionHeldReason::ServerDraining)
        );
    }
    let response = received.await.unwrap();
    if matches!(phase, FailurePhase::InitialFlush) {
        assert!(matches!(
            response.unwrap(),
            ThreadAttentionResponse::Accepted { .. }
        ));
    } else {
        assert!(matches!(response, Err(AttentionError::Stopped)));
    }
    assert!(matches!(
        session
            .services
            .attention_ledger
            .snapshot(session.thread_id, session.live_thread())
            .await,
        Err(AttentionError::Stopped)
    ));
    // Generic ShutdownComplete and ThreadNotFound do not certify handoff.
    let live = reacquire_fixture_writer(&session, &store, path.clone()).await;
    let (items, _, errors) = RolloutRecorder::load_rollout_items(&path).await.unwrap();
    assert_eq!(errors, 0);
    assert_eq!(
        items
            .iter()
            .filter(|item| matches!(item, RolloutItem::EventMsg(EventMsg::ThreadAttention(_))))
            .count(),
        if matches!(phase, FailurePhase::InitialFlush) {
            4
        } else {
            2
        }
    );
    let cold = crate::attention::AttentionLedger::default()
        .snapshot(session.thread_id, Some(&live))
        .await
        .unwrap();
    assert_eq!(
        cold.len(),
        if matches!(phase, FailurePhase::InitialFlush) {
            2
        } else {
            1
        }
    );
    assert!(cold.iter().any(|notice| notice.batch_id == "prior"));
    assert!(
        cold.iter()
            .all(|notice| matches!(notice.admission, ThreadAttentionResponse::Accepted { .. }))
    );
    assert_eq!(
        cold.iter().any(|notice| notice.batch_id == "queued"),
        matches!(phase, FailurePhase::InitialFlush)
    );
    live.shutdown().await.unwrap();
}

fn raw_fixture_context(marker: &'static str) -> crate::context::InternalModelContextFragment {
    crate::context::InternalModelContextFragment::new(
        crate::context::InternalContextSource::from_static("attention_fixture"),
        marker,
    )
}

async fn raw_fixture_thread(
    session: Arc<Session>,
    events: async_channel::Receiver<Event>,
) -> (Arc<crate::CodexThread>, async_channel::Receiver<Submission>) {
    let context = session
        .active_turn
        .lock()
        .await
        .as_ref()
        .unwrap()
        .task
        .as_ref()
        .unwrap()
        .turn_context
        .clone();
    let (mut thread, old_receiver) =
        make_thread_for_session(session, &context, events, SessionSource::Exec).await;
    let (sender, receiver) = async_channel::bounded(4);
    thread.io.tx_sub = sender;
    drop(old_receiver);
    (Arc::new(thread), receiver)
}

fn contains_raw_fixture_input(input: &[TurnInput], marker: &'static str) -> bool {
    let expected = crate::context::ContextualUserFragment::into(raw_fixture_context(marker));
    input.iter().any(|item| {
        matches!(item, TurnInput::ResponseItem(envelope)
            if envelope.item == expected && envelope.metadata.is_none())
    })
}

#[tokio::test]
async fn canonical_attention_public_raw_queue_appends_before_shutdown_without_receipt() {
    let (session, _store, events) = running_fixture().await;
    let (thread, receiver) = raw_fixture_thread(session.clone(), events).await;
    let config = session.get_config().await;
    let mut call =
        Box::pin(thread.inject_attention_if_running(raw_fixture_context("raw-before-shutdown")));
    assert!(futures::poll!(&mut call).is_pending());
    assert_eq!(receiver.len(), 1, "public helper actually enqueued");
    let dispatcher = tokio::spawn(crate::session::handlers::submission_loop(
        session.clone(),
        config,
        receiver,
    ));
    assert_eq!(call.await.unwrap().unwrap(), "turn_id");
    let consumed = session
        .input_queue
        .get_pending_input(&session.active_turn)
        .await
        .0;
    assert!(contains_raw_fixture_input(&consumed, "raw-before-shutdown"));
    assert_eq!(
        thread.attention_snapshot().await.unwrap().len(),
        1,
        "raw injection invents no canonical intent"
    );
    thread.submit(Op::Shutdown).await.unwrap();
    dispatcher.await.unwrap();
}

#[test_case::test_case(false; "queued_after_shutdown")]
#[test_case::test_case(true; "biased_tree_cancellation")]
#[tokio::test]
async fn canonical_attention_public_raw_terminal_queue_drains_without_append(tree_cancel: bool) {
    let (session, _store, events) = running_fixture().await;
    let (thread, receiver) = raw_fixture_thread(session.clone(), events).await;
    if !tree_cancel {
        thread.submit(Op::Shutdown).await.unwrap();
    }
    let mut call =
        Box::pin(thread.inject_attention_if_running(raw_fixture_context("raw-after-terminal")));
    assert!(futures::poll!(&mut call).is_pending());
    if tree_cancel {
        session.services.local_agent_runtime.request_shutdown();
    }
    crate::session::handlers::submission_loop(
        session.clone(),
        session.get_config().await,
        receiver,
    )
    .await;
    assert_eq!(
        call.await.unwrap(),
        Err(codex_protocol::attention::ThreadAttentionHeldReason::ServerDraining)
    );
    let consumed = session
        .input_queue
        .get_pending_input(&session.active_turn)
        .await
        .0;
    assert!(!contains_raw_fixture_input(&consumed, "raw-after-terminal"));
    // Channel is now closed: this separate call was known never-enqueued.
    assert_eq!(
        thread
            .inject_attention_if_running(raw_fixture_context("raw-never-enqueued"))
            .await
            .unwrap(),
        Err(codex_protocol::attention::ThreadAttentionHeldReason::ServerDraining)
    );
}

#[tokio::test]
async fn canonical_attention_public_raw_dropped_waiter_does_not_retract_dispatch() {
    let (session, _store, events) = running_fixture().await;
    let (thread, receiver) = raw_fixture_thread(session.clone(), events).await;
    let mut dropped =
        Box::pin(thread.inject_attention_if_running(raw_fixture_context("raw-dropped-observer")));
    assert!(futures::poll!(&mut dropped).is_pending());
    drop(dropped);
    let mut barrier =
        Box::pin(thread.inject_attention_if_running(raw_fixture_context("raw-observed-barrier")));
    assert!(futures::poll!(&mut barrier).is_pending());
    let dispatcher = tokio::spawn(crate::session::handlers::submission_loop(
        session.clone(),
        session.get_config().await,
        receiver,
    ));
    barrier.await.unwrap().unwrap();
    // This native API consumes pending input. Retain one real consumption result
    // so both predicates inspect the same admitted queue, not a second empty one.
    let consumed = session
        .input_queue
        .get_pending_input(&session.active_turn)
        .await
        .0;
    assert!(contains_raw_fixture_input(
        &consumed,
        "raw-dropped-observer"
    ));
    assert!(contains_raw_fixture_input(
        &consumed,
        "raw-observed-barrier"
    ));
    assert_eq!(thread.attention_snapshot().await.unwrap().len(), 1);
    thread.submit(Op::Shutdown).await.unwrap();
    dispatcher.await.unwrap();
}

#[tokio::test]
async fn canonical_attention_public_raw_lost_reply_is_outer_unknown_after_actual_append() {
    let (session, store, events) = running_fixture().await;
    let (thread, receiver) = raw_fixture_thread(session.clone(), events).await;
    let mut call = Box::pin(
        thread.inject_attention_if_running(raw_fixture_context("raw-unknown-observation")),
    );
    assert!(futures::poll!(&mut call).is_pending());
    let submission = receiver.recv().await.unwrap();
    let Op::InjectAttention { item, reply } = submission.op else {
        panic!("public helper queued its typed operation")
    };
    // Controlled observation fault: use the real private append, then lose its
    // reply. This is not a claim that a native dispatcher panic was reproduced.
    session.inject_attention_if_running(*item).await.unwrap();
    drop(reply);
    let error = call.await.unwrap_err();
    assert!(matches!(
        error.details(),
        codex_protocol::error::CodexErrorDetails::InternalAgentDied
    ));
    let consumed = session
        .input_queue
        .get_pending_input(&session.active_turn)
        .await
        .0;
    assert!(
        contains_raw_fixture_input(&consumed, "raw-unknown-observation"),
        "outer error is not proof of non-admission or safe retry"
    );
    assert_eq!(thread.attention_snapshot().await.unwrap().len(), 1);
    crate::session::handlers::shutdown_session_runtime(&session).await;
    store.fail_close.store(false, Ordering::SeqCst);
    session.live_thread().unwrap().shutdown().await.unwrap();
}

#[tokio::test]
async fn canonical_attention_public_raw_after_successful_suspend_is_confirmed_unprocessed() {
    let (session, store, events) = running_fixture().await;
    let path = fixture_rollout_path(&session, &store).await;
    let (thread, receiver) = raw_fixture_thread(session.clone(), events).await;
    let (reply, result) = tokio::sync::oneshot::channel();
    thread
        .submit(Op::SuspendTurnAndShutdown { reply })
        .await
        .unwrap();
    let mut raw = Box::pin(
        thread.inject_attention_if_running(raw_fixture_context("raw-after-successful-suspend")),
    );
    assert!(futures::poll!(&mut raw).is_pending());
    assert_eq!(receiver.len(), 2);
    crate::session::handlers::submission_loop(
        session.clone(),
        session.get_config().await,
        receiver,
    )
    .await;
    assert!(matches!(
        result.await.unwrap().unwrap(),
        codex_protocol::turn_input::SuspendTurnOutcome::Suspended { .. }
    ));
    assert_eq!(
        raw.await.unwrap(),
        Err(codex_protocol::attention::ThreadAttentionHeldReason::ServerDraining)
    );
    let live = reacquire_fixture_writer(&session, &store, path).await;
    let cold = crate::attention::AttentionLedger::default()
        .snapshot(session.thread_id, Some(&live))
        .await
        .unwrap();
    assert_eq!(
        cold.len(),
        1,
        "raw operation creates no fabricated intent at the successful terminal boundary"
    );
    assert_eq!(cold[0].batch_id, "prior");
    live.shutdown().await.unwrap();
}
