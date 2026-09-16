use std::time::Duration;

use tokio::time::Instant;

pub(crate) const BASE_COOLDOWN: Duration = Duration::from_secs(30);
const MAX_COOLDOWN: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Default)]
pub(crate) struct WebsocketCircuitBreaker {
    consecutive_openings: u32,
    open_until: Option<Instant>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct CircuitOpenResult {
    pub(crate) newly_opened: bool,
    pub(crate) cooldown: Duration,
}

impl WebsocketCircuitBreaker {
    pub(crate) fn allows_attempt(&self, now: Instant) -> bool {
        self.open_until.is_none_or(|open_until| now >= open_until)
    }

    pub(crate) fn open(&mut self, now: Instant) -> CircuitOpenResult {
        if let Some(open_until) = self.open_until
            && now < open_until
        {
            return CircuitOpenResult {
                newly_opened: false,
                cooldown: open_until.duration_since(now),
            };
        }

        self.consecutive_openings = self.consecutive_openings.saturating_add(1);
        let exponent = self.consecutive_openings.saturating_sub(1).min(4);
        let cooldown = BASE_COOLDOWN
            .saturating_mul(1_u32 << exponent)
            .min(MAX_COOLDOWN);
        self.open_until = Some(now + cooldown);

        CircuitOpenResult {
            newly_opened: true,
            cooldown,
        }
    }

    pub(crate) fn record_success(&mut self) {
        self.consecutive_openings = 0;
        self.open_until = None;
    }
}

#[cfg(test)]
#[path = "websocket_circuit_tests.rs"]
mod tests;
