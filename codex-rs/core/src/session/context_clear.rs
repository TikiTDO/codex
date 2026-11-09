//! Idle effective-context replacement: persist the prepared checkpoint before installation.

use super::handlers::CONTEXT_CLEARED_MARKER;
use super::session::Session;
use super::thread_settings;
use crate::context::CompactionSummary;
use crate::context::ContextualUserFragment;
use crate::context::UserGoalUpdate;
use codex_history::CompactedItem;
use codex_history::CompactionResumeMetadata;
use codex_history::DynamicToolsSnapshot;
use codex_history::ResponseItemEnvelope;
use codex_history::RolloutItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::TokenCountEvent;
use codex_protocol::protocol::TokenUsage;
use codex_protocol::protocol::TokenUsageInfo;
use codex_thread_store::PersistContext;

impl Session {
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "idle admission and state stay fenced through the durable checkpoint barrier"
    )]
    pub(crate) async fn clear_effective_context(&self) -> anyhow::Result<Option<TokenCountEvent>> {
        let live_thread = self.live_thread_for_persistence("clear context")?;
        let _settings = thread_settings::acquire_persistence_lock(self).await;
        let _active = self.active_turn.lock().await;
        anyhow::ensure!(
            _active.is_none(),
            "Cannot clear context while a turn is active, reserved, or finishing."
        );
        // Capture state-reading inputs before taking the state guard through I/O.
        let settings = thread_settings::applied_event(self).await;
        let base_instructions = self.get_base_instructions().await;
        live_thread.persist(PersistContext::Standard).await?;
        let mut state = self.state.lock().await;
        let mut marker = ResponseItemEnvelope::new(ContextualUserFragment::into(
            CompactionSummary::new(CONTEXT_CLEARED_MARKER.to_string()),
        ));
        Self::assign_missing_response_item_id(&mut marker.item);
        let mcp_checkpoint = self
            .services
            .executed_tool_calls
            .mcp_attribution_checkpoint(true);
        if let Some((attribution, _)) = &mcp_checkpoint {
            marker.metadata.get_or_insert_default().mcp_attribution = Some(attribution.clone());
        }
        let mut items = vec![marker];
        // Goal edits have their own lifetime; clearing a prompt does not erase them.
        items.extend(
            state
                .history
                .annotated_items()
                .iter()
                .filter(|envelope| UserGoalUpdate::message_text(&envelope.item).is_some())
                .cloned(),
        );
        let mut prepared = state.prepare_context_clear(items);
        let (window_number, window_ids) = (prepared.window_number, prepared.window_ids);
        let count = prepared
            .history
            .estimate_token_count_with_base_instructions(&base_instructions)
            .map(|estimate| {
                let mut info = state.token_info().unwrap_or(TokenUsageInfo {
                    total_token_usage: TokenUsage::default(),
                    last_usage_includes_reasoning: None,
                    last_token_usage: TokenUsage::default(),
                    model_context_window: None,
                });
                info.last_token_usage = TokenUsage {
                    total_tokens: estimate.max(0),
                    ..TokenUsage::default()
                };
                info.last_usage_includes_reasoning = Some(true);
                let covered_items = prepared.history.raw_items().len();
                prepared
                    .history
                    .set_token_info_with_boundary(Some(info.clone()), covered_items);
                TokenCountEvent {
                    info: Some(info),
                    rate_limits: state.token_info_and_rate_limits().1,
                }
            });
        let checkpoint = CompactedItem {
            message: "context cleared".into(),
            replacement_history: Some(prepared.history.annotated_items().to_vec()),
            guardian_history: prepared.history.guardian_history_checkpoint(),
            retained_context: Some(prepared.history.retained_context().clone()),
            mcp_resource_origins: self.services.mcp_runtime.resource_origin_checkpoint(),
            window_number: Some(window_number),
            first_window_id: Some(window_ids.first_window_id.to_string()),
            previous_window_id: window_ids.previous_window_id.map(|id| id.to_string()),
            window_id: Some(window_ids.window_id.to_string()),
            compaction_response_id: None,
            latest_token_usage_record: state.latest_token_usage_record.clone(),
            resume_metadata: Some(CompactionResumeMetadata {
                dynamic_tools: Some(Box::new(DynamicToolsSnapshot {
                    thread_id: self.thread_id(),
                    tools: state.persisted_dynamic_tools.to_vec(),
                })),
                multi_agent_version: self.multi_agent_version(),
                last_started_turn_id: state.last_started_turn_id.clone(),
                turn_attribution: state.turn_attribution.clone(),
                previous_turn_settings: state.previous_turn_settings(),
            }),
        };
        // No stale TurnContext or WorldState follows this checkpoint. Cold resume must
        // rebuild initial context, just as the live history's absent reference requires.
        let mut records = vec![
            RolloutItem::Compacted(Box::new(checkpoint)),
            RolloutItem::EventMsg(settings),
        ];
        if let Some(count) = &count {
            records.push(RolloutItem::EventMsg(EventMsg::TokenCount(count.clone())));
        }
        live_thread.append_items(&records).await?;
        live_thread.flush().await?;
        // No await between the successful barrier and live installation. A failed or
        // cancelled I/O may still have reached disk; it never claims an in-memory rollback.
        state.install_context_clear(prepared);
        if let Some((_, revision)) = mcp_checkpoint {
            self.services
                .executed_tool_calls
                .mark_mcp_attribution_persisted(revision);
        }
        Ok(count)
    }
}
