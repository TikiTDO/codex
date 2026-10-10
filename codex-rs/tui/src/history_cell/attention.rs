//! Independent runtime notices: source details remain inspectable without making
//! a UserMessage or changing question-editor state. Replay updates one batch cell.

use super::*;
use codex_app_server_protocol::ThreadAttentionDelivery;
use codex_app_server_protocol::ThreadAttentionHeldReason;
use codex_app_server_protocol::ThreadAttentionNotification;
use codex_app_server_protocol::ThreadAttentionResponse;
use std::sync::Mutex;

#[derive(Debug)]
pub(crate) struct AttentionHistoryCell {
    pub(crate) notice: Arc<Mutex<ThreadAttentionNotification>>,
    pub(crate) batch_id: String,
}

impl AttentionHistoryCell {
    fn lines(&self, details: bool) -> Vec<Line<'static>> {
        let notice = self.notice.lock().expect("attention notice lock");
        let state = match &notice.admission {
            ThreadAttentionResponse::Accepted { delivery, .. } => match delivery {
                ThreadAttentionDelivery::Started => "background turn started",
                ThreadAttentionDelivery::Buffered => "context buffered",
            },
            ThreadAttentionResponse::Held { reason } => match reason {
                ThreadAttentionHeldReason::BlockingRequest => "held for your answer or approval",
                ThreadAttentionHeldReason::Busy => "held while the task is unavailable",
                ThreadAttentionHeldReason::PlanMode => "held in plan mode",
                ThreadAttentionHeldReason::PendingTriggerTurn => "held for a higher-priority turn",
                ThreadAttentionHeldReason::ServerDraining => "held while the server drains",
                ThreadAttentionHeldReason::AdmissionUncertain => {
                    "held: admission receipt uncertain"
                }
            },
        };
        let source = notice
            .origins
            .first()
            .map(|origin| origin.sender.as_deref().unwrap_or(&origin.source_ref))
            .unwrap_or("runtime");
        let source = source.chars().take(40).collect::<String>();
        let more = notice.origins.len().saturating_sub(1);
        let suffix = if more > 0 {
            format!(" +{more}")
        } else {
            String::new()
        };
        let mut lines = vec![Line::from(format!(
            "Attention · {source}{suffix} · {state}"
        ))];
        if details {
            lines.push(Line::from(format!(
                "Batch: {}. Admission is not proof of model consumption.",
                notice.batch_id
            )));
            for origin in &notice.origins {
                lines.push(Line::from(format!(
                    "Read {:?}: {}{}",
                    origin.kind,
                    origin.source_ref,
                    origin
                        .sender
                        .as_ref()
                        .map(|s| format!(" from {s}"))
                        .unwrap_or_default()
                )));
            }
        }
        lines
    }
}

impl HistoryCell for AttentionHistoryCell {
    fn display_lines(&self, width: u16) -> Vec<Line<'static>> {
        adaptive_wrap_lines(self.lines(false), RtOptions::new(usize::from(width.max(1))))
    }

    fn raw_lines(&self) -> Vec<Line<'static>> {
        self.lines(true)
    }

    fn live_raw_lines(&self) -> Vec<Line<'static>> {
        self.lines(false)
    }

    fn transcript_lines(&self, width: u16) -> Vec<Line<'static>> {
        adaptive_wrap_lines(self.lines(true), RtOptions::new(usize::from(width.max(1))))
    }

    fn activity_ids(&self) -> Vec<String> {
        vec![format!("attention:{}", self.batch_id)]
    }

    fn has_stable_transcript_height(&self) -> bool {
        false
    }
}
