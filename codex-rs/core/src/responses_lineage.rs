use codex_api::ResponsesApiRequest;
use codex_protocol::models::ResponseItem;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;

#[derive(Debug, Clone)]
pub(crate) struct LastResponse {
    pub(crate) response_id: String,
    pub(crate) items_added: Vec<ResponseItem>,
}

#[derive(Debug)]
pub(crate) enum ResponsesLineageUpdate {
    Completed(LastResponse),
    PreviousResponseNotFound,
}

#[derive(Debug)]
struct ConfirmedResponse {
    request: ResponsesApiRequest,
    response: LastResponse,
    from_untraced_warmup: bool,
}

#[derive(Debug)]
struct PendingResponse {
    request: ResponsesApiRequest,
    receiver: oneshot::Receiver<ResponsesLineageUpdate>,
    from_untraced_warmup: bool,
}

#[derive(Debug)]
pub(crate) struct IncrementalResponseRequest {
    pub(crate) previous_response_id: String,
    pub(crate) input: Vec<ResponseItem>,
    pub(crate) previous_response_from_untraced_warmup: bool,
}

/// Tracks the last server-confirmed Responses state independently of its transport connection.
///
/// A pending request never replaces the confirmed baseline until `response.completed` arrives.
/// This lets an HTTP retry or a replacement WebSocket continue from the last durable response
/// rather than expanding a failed delta back into the complete local history.
#[derive(Debug, Default)]
pub(crate) struct ResponsesLineage {
    confirmed: Option<ConfirmedResponse>,
    pending: Option<PendingResponse>,
}

impl ResponsesLineage {
    pub(crate) fn has_state(&self) -> bool {
        self.confirmed.is_some() || self.pending.is_some()
    }

    pub(crate) fn prepare_incremental_request(
        &mut self,
        request: &ResponsesApiRequest,
        allow_empty_delta: bool,
    ) -> Option<IncrementalResponseRequest> {
        self.promote_pending();
        let confirmed = self.confirmed.as_ref()?;
        if !responses_request_properties_match(&confirmed.request, request) {
            tracing::trace!("incremental request failed, response properties did not match");
            return None;
        }

        let previous_items_len = confirmed
            .request
            .input
            .len()
            .checked_add(confirmed.response.items_added.len())?;
        let Some((request_items_to_compare, incremental_items)) =
            request.input.split_at_checked(previous_items_len)
        else {
            tracing::trace!("incremental request failed, incompatible request length");
            return None;
        };
        let previous_items = confirmed
            .request
            .input
            .iter()
            .chain(&confirmed.response.items_added);
        if !previous_items
            .zip(request_items_to_compare)
            .all(|(previous, current)| {
                response_items_equal_ignoring_internal_metadata(previous, current)
            })
        {
            tracing::trace!("incremental request failed, items did not match");
            return None;
        }
        if !allow_empty_delta && incremental_items.is_empty() {
            return None;
        }
        if confirmed.response.response_id.is_empty() {
            tracing::trace!("incremental request failed, no previous response id");
            return None;
        }

        Some(IncrementalResponseRequest {
            previous_response_id: confirmed.response.response_id.clone(),
            input: incremental_items.to_vec(),
            previous_response_from_untraced_warmup: confirmed.from_untraced_warmup,
        })
    }

    pub(crate) fn record_pending(
        &mut self,
        request: ResponsesApiRequest,
        receiver: oneshot::Receiver<ResponsesLineageUpdate>,
        from_untraced_warmup: bool,
    ) {
        self.pending = Some(PendingResponse {
            request,
            receiver,
            from_untraced_warmup,
        });
    }

    pub(crate) fn clear(&mut self) {
        *self = Self::default();
    }

    fn promote_pending(&mut self) {
        let Some(mut pending) = self.pending.take() else {
            return;
        };
        match pending.receiver.try_recv() {
            Ok(ResponsesLineageUpdate::Completed(response)) => {
                self.confirmed = Some(ConfirmedResponse {
                    request: pending.request,
                    response,
                    from_untraced_warmup: pending.from_untraced_warmup,
                });
            }
            Ok(ResponsesLineageUpdate::PreviousResponseNotFound) => {
                self.confirmed = None;
            }
            Err(TryRecvError::Empty) => self.pending = Some(pending),
            Err(TryRecvError::Closed) => {}
        }
    }
}

// Request equality includes input and metadata, while lineage reuse compares input separately and
// ignores per-attempt metadata. Keep this exhaustive so every new request field gets an explicit
// state-reuse decision.
fn responses_request_properties_match(
    previous: &ResponsesApiRequest,
    current: &ResponsesApiRequest,
) -> bool {
    let ResponsesApiRequest {
        model: previous_model,
        instructions: previous_instructions,
        previous_response_id: _,
        input: _,
        tools: previous_tools,
        tool_choice: previous_tool_choice,
        parallel_tool_calls: previous_parallel_tool_calls,
        reasoning: previous_reasoning,
        store: previous_store,
        stream: previous_stream,
        stream_options: _,
        include: previous_include,
        service_tier: previous_service_tier,
        prompt_cache_key: previous_prompt_cache_key,
        text: previous_text,
        client_metadata: _,
        access_programs: _,
    } = previous;
    let ResponsesApiRequest {
        model: current_model,
        instructions: current_instructions,
        previous_response_id: _,
        input: _,
        tools: current_tools,
        tool_choice: current_tool_choice,
        parallel_tool_calls: current_parallel_tool_calls,
        reasoning: current_reasoning,
        store: current_store,
        stream: current_stream,
        stream_options: _,
        include: current_include,
        service_tier: current_service_tier,
        prompt_cache_key: current_prompt_cache_key,
        text: current_text,
        client_metadata: _,
        access_programs: _,
    } = current;

    previous_model == current_model
        && previous_instructions == current_instructions
        && previous_tools == current_tools
        && previous_tool_choice == current_tool_choice
        && previous_parallel_tool_calls == current_parallel_tool_calls
        && previous_reasoning == current_reasoning
        && previous_store == current_store
        && previous_stream == current_stream
        // Stream options control delivery for this response, not referenced context.
        && previous_include == current_include
        && previous_service_tier == current_service_tier
        && previous_prompt_cache_key == current_prompt_cache_key
        && previous_text == current_text
}

fn response_items_equal_ignoring_internal_metadata(
    previous: &ResponseItem,
    current: &ResponseItem,
) -> bool {
    if previous == current {
        return true;
    }

    let mut previous = previous.clone();
    previous.clear_internal_chat_message_metadata_passthrough();
    let mut current = current.clone();
    current.clear_internal_chat_message_metadata_passthrough();
    previous == current
}
