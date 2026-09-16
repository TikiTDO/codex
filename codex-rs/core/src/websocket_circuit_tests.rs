use super::BASE_COOLDOWN;
use super::CircuitOpenResult;
use super::WebsocketCircuitBreaker;
use pretty_assertions::assert_eq;
use tokio::time::Instant;

#[test]
fn circuit_recovers_and_backs_off_until_success() {
    let now = Instant::now();
    let mut circuit = WebsocketCircuitBreaker::default();
    assert!(circuit.allows_attempt(now));

    assert_eq!(
        circuit.open(now),
        CircuitOpenResult {
            newly_opened: true,
            cooldown: BASE_COOLDOWN,
        }
    );
    assert!(!circuit.allows_attempt(now + BASE_COOLDOWN / 2));
    assert!(circuit.allows_attempt(now + BASE_COOLDOWN));

    let second_attempt = now + BASE_COOLDOWN;
    assert_eq!(
        circuit.open(second_attempt),
        CircuitOpenResult {
            newly_opened: true,
            cooldown: BASE_COOLDOWN * 2,
        }
    );
    assert_eq!(
        circuit.open(second_attempt),
        CircuitOpenResult {
            newly_opened: false,
            cooldown: BASE_COOLDOWN * 2,
        }
    );

    circuit.record_success();
    assert!(circuit.allows_attempt(second_attempt));
    assert_eq!(
        circuit.open(second_attempt),
        CircuitOpenResult {
            newly_opened: true,
            cooldown: BASE_COOLDOWN,
        }
    );
}
