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
