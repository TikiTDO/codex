//! Checks incoming event ownership before consulting native-thread caches or UI state.
//! Connection-wide lifecycle and account events may only update the displayed conversation's UI.

use super::App;
use super::app_server_event_targets::ServerNotificationThreadTarget;
use super::app_server_event_targets::server_notification_thread_target;
use crate::app_server_session::AppServerSession;
use crate::app_server_session::AppServerSessionEvent;
use crate::backend_id::BackendId;
use codex_app_server_client::AppServerEvent;
use codex_app_server_protocol::ServerNotification;

impl App {
    pub(super) async fn handle_polled_backend_event(
        &mut self,
        app_server: &AppServerSession,
        displayed_owner: BackendId,
        event: AppServerSessionEvent,
    ) {
        if displayed_owner != event.backend_id {
            // A parked voice session still needs its own notifications.
            if event.backend_id == app_server.backend_id()
                && let AppServerEvent::ServerNotification(notification) = &event.event
                && let ServerNotificationThreadTarget::Thread(thread_id) =
                    server_notification_thread_target(notification)
                && self.voice_owner_thread_id() == Some(thread_id)
            {
                // Backend switching retains the voice owner's existing channel
                // and stamps its source. Never update a colliding foreign cache
                // or feed this event into the displayed thread's active receiver.
                let owned_channel = self
                    .thread_event_channels
                    .get(&thread_id)
                    .filter(|channel| channel.backend_id == Some(event.backend_id));
                if let Some(channel) = owned_channel {
                    let mut store = channel.store.lock().await;
                    if store.suppresses_attention_marker(notification) {
                        return;
                    }
                    // Genuine input and every native horizon mutation must reach
                    // the same owner even while another backend is displayed.
                    store.push_notification_ref(notification);
                } else if matches!(
                    notification.as_ref(),
                    ServerNotification::AttentionTurnStarted(_)
                ) {
                    tracing::warn!(%thread_id, "background voice attention provenance unavailable: source channel is not bound");
                    return;
                }
                self.deliver_background_voice_notification(thread_id, notification);
            }
            return;
        }
        self.handle_backend_event(app_server, event).await;
    }

    pub(super) async fn handle_backend_event(
        &mut self,
        app_server: &AppServerSession,
        event: AppServerSessionEvent,
    ) {
        if event.backend_id != app_server.backend_id() {
            tracing::warn!("ignoring server event from a different backend");
            return;
        }
        self.handle_app_server_event(app_server, event.event).await;
    }
}
