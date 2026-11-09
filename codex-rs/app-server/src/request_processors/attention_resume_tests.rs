//! Publication join only; native scoped source/capture has store witnesses.
use super::*;

fn presentation(target: ThreadId) -> codex_thread_store::CanonicalAttentionPresentation {
    codex_thread_store::CanonicalAttentionPresentation {
        thread_id: target,
        markers: vec![codex_app_server_protocol::AttentionTurnStartedNotification {
            thread_id: target.to_string(), turn_id: "t".into(),
        }],
        foreground_turn_ids: HashSet::new(), retained_turn_ids: HashSet::from(["t".into()]),
        coverage: Vec::new(), scope: Vec::new(),
    }
}
#[test]
fn quiet_attention_resume_join_preserves_background_and_both_foreground_vetoes() {
    let target = ThreadId::new();
    assert_eq!(attention_resume_markers(target, presentation(target), HashSet::new(), None).unwrap().len(), 1);
    assert!(attention_resume_markers(target, presentation(target), HashSet::from(["t".into()]), None).unwrap().is_empty());
    let mut source = presentation(target);
    source.foreground_turn_ids.insert("t".into());
    assert!(attention_resume_markers(target, source, HashSet::new(), None).unwrap().is_empty());
}
#[test]
fn quiet_attention_resume_join_requires_retained_or_real_active_turn_not_marker() {
    let target = ThreadId::new();
    let mut source = presentation(target);
    source.retained_turn_ids.clear();
    assert!(attention_resume_markers(target, source, HashSet::new(), None).unwrap().is_empty());
    let mut source = presentation(target);
    source.retained_turn_ids.clear();
    assert_eq!(attention_resume_markers(target, source, HashSet::new(), Some("t".into())).unwrap().len(), 1);
}
#[test]
fn quiet_attention_resume_join_refuses_wrong_source_and_foreign_marker() {
    let target = ThreadId::new();
    assert!(attention_resume_markers(target, presentation(ThreadId::new()), HashSet::new(), None).is_none());
    let mut source = presentation(target);
    source.markers[0].thread_id = ThreadId::new().to_string();
    assert!(attention_resume_markers(target, source, HashSet::new(), None).unwrap().is_empty());
}
