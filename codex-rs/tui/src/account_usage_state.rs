use codex_app_server_protocol::GetAccountRateLimitsResponse;
use codex_app_server_protocol::RateLimitSnapshot;
use codex_utils_home_dir::find_codex_home;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

const SNAPSHOT_SCHEMA: &str = "codex.account-usage-state/v1";
const SNAPSHOT_DIRECTORY: &str = "account-usage";
const SNAPSHOT_FILE: &str = "current.json";
static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Serialize)]
struct AccountUsageSnapshot {
    schema: &'static str,
    updated_at_unix_ms: u64,
    process: ProcessIdentity,
    ordinary_usage_allowed: Option<bool>,
    rate_limits_by_limit_id: BTreeMap<String, UsageLimitSnapshot>,
    rate_limit_reset_credits_available: Option<i64>,
}

#[derive(Serialize)]
struct ProcessIdentity {
    pid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    linux_start_ticks: Option<u64>,
}

#[derive(Serialize)]
struct UsageLimitSnapshot {
    limit_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    primary: Option<UsageWindow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    secondary: Option<UsageWindow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    credits: Option<UsageCredits>,
}

#[derive(Serialize)]
struct UsageWindow {
    used_percent: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    window_minutes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    resets_at: Option<i64>,
}

#[derive(Serialize)]
struct UsageCredits {
    has_credits: bool,
    unlimited: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    balance: Option<String>,
}

fn usage_limit(limit_id: String, snapshot: &RateLimitSnapshot) -> UsageLimitSnapshot {
    UsageLimitSnapshot {
        limit_id,
        name: snapshot.limit_name.clone(),
        primary: snapshot.primary.as_ref().map(|window| UsageWindow {
            used_percent: window.used_percent,
            window_minutes: window.window_duration_mins,
            resets_at: window.resets_at,
        }),
        secondary: snapshot.secondary.as_ref().map(|window| UsageWindow {
            used_percent: window.used_percent,
            window_minutes: window.window_duration_mins,
            resets_at: window.resets_at,
        }),
        credits: snapshot.credits.as_ref().map(|credits| UsageCredits {
            has_credits: credits.has_credits,
            unlimited: credits.unlimited,
            balance: credits.balance.clone(),
        }),
    }
}

/// Publish the newest accepted account usage read for local status consumers.
///
/// Account identifiers, backend banners, reset-credit identifiers, and descriptions are
/// deliberately excluded. The snapshot is current state, not an authorization or accounting
/// ledger, and failure to write it must never break the TUI's usage path.
pub(crate) fn publish(response: &GetAccountRateLimitsResponse) {
    let Ok(codex_home) = find_codex_home() else {
        return;
    };
    let path = codex_home.join(SNAPSHOT_DIRECTORY).join(SNAPSHOT_FILE);
    let mut source_limits = response.rate_limits_by_limit_id.clone().unwrap_or_default();
    let primary_id = response
        .rate_limits
        .limit_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .unwrap_or("codex")
        .to_string();
    source_limits
        .entry(primary_id)
        .or_insert_with(|| response.rate_limits.clone());
    let rate_limits_by_limit_id = source_limits
        .iter()
        .map(|(limit_id, snapshot)| (limit_id.clone(), usage_limit(limit_id.clone(), snapshot)))
        .collect::<BTreeMap<_, _>>();
    let snapshot = AccountUsageSnapshot {
        schema: SNAPSHOT_SCHEMA,
        updated_at_unix_ms: unix_time_ms(),
        process: ProcessIdentity {
            pid: std::process::id(),
            linux_start_ticks: linux_process_start_ticks(std::process::id()),
        },
        ordinary_usage_allowed: response.ordinary_usage_allowed,
        rate_limits_by_limit_id,
        rate_limit_reset_credits_available: response
            .rate_limit_reset_credits
            .as_ref()
            .map(|summary| summary.available_count),
    };
    let temporary_sequence = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let _ = write_snapshot(&path, &snapshot, temporary_sequence);
}

fn write_snapshot(
    path: &Path,
    snapshot: &AccountUsageSnapshot,
    temporary_sequence: u64,
) -> std::io::Result<()> {
    let Some(parent) = path.parent() else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "account usage snapshot has no parent directory",
        ));
    };
    fs::create_dir_all(parent)?;
    if !fs::symlink_metadata(parent)?.file_type().is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "account usage snapshot directory is not a directory",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
    }
    let temporary = parent.join(format!(
        ".{SNAPSHOT_FILE}.{}.{}.tmp",
        std::process::id(),
        temporary_sequence,
    ));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(fs::Permissions::from_mode(0o600))?;
        }
        serde_json::to_writer(&mut file, snapshot).map_err(std::io::Error::other)?;
        file.write_all(b"\n")?;
        file.flush()?;
        fs::rename(&temporary, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(target_os = "linux")]
fn linux_process_start_ticks(pid: u32) -> Option<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let after_name = stat.rsplit_once(')')?.1.trim_start();
    after_name.split_whitespace().nth(19)?.parse().ok()
}

#[cfg(not(target_os = "linux"))]
fn linux_process_start_ticks(_pid: u32) -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_app_server_protocol::CreditsSnapshot;
    use codex_app_server_protocol::RateLimitResetCredit;
    use codex_app_server_protocol::RateLimitResetCreditStatus;
    use codex_app_server_protocol::RateLimitResetCreditsSummary;
    use codex_app_server_protocol::RateLimitResetType;
    use codex_app_server_protocol::RateLimitWindow;
    use serde_json::Value;
    use tempfile::TempDir;

    #[test]
    fn snapshot_keeps_usage_and_omits_account_and_reset_credit_identity() {
        let temp = TempDir::new().expect("temp dir");
        let path = temp.path().join("account-usage/current.json");
        let response = GetAccountRateLimitsResponse {
            ordinary_usage_allowed: Some(true),
            rate_limits: RateLimitSnapshot {
                limit_id: Some("codex".to_string()),
                limit_name: None,
                normal_model_slug: None,
                primary: Some(RateLimitWindow {
                    used_percent: 43,
                    window_duration_mins: Some(300),
                    resets_at: Some(1_789_522_200),
                }),
                secondary: Some(RateLimitWindow {
                    used_percent: 43,
                    window_duration_mins: Some(10_080),
                    resets_at: Some(1_789_522_200),
                }),
                credits: Some(CreditsSnapshot {
                    has_credits: true,
                    unlimited: false,
                    balance: Some("4632.0".to_string()),
                }),
                individual_limit: None,
                spend_control_reached: Some(false),
                plan_type: None,
                rate_limit_reached_type: None,
            },
            rate_limits_by_limit_id: None,
            rate_limit_reset_credits: Some(RateLimitResetCreditsSummary {
                available_count: 1,
                credits: Some(vec![RateLimitResetCredit {
                    id: "do-not-persist".to_string(),
                    reset_type: RateLimitResetType::CodexRateLimits,
                    status: RateLimitResetCreditStatus::Available,
                    granted_at: 1,
                    expires_at: None,
                    title: Some("private detail".to_string()),
                    description: Some("private detail".to_string()),
                }]),
            }),
            account_id: Some("do-not-persist".to_string()),
            rate_limit_upsell: Some(serde_json::json!({"message": "do-not-persist"})),
        };
        let snapshot = AccountUsageSnapshot {
            schema: SNAPSHOT_SCHEMA,
            updated_at_unix_ms: 1_789_000_000_000,
            process: ProcessIdentity {
                pid: std::process::id(),
                linux_start_ticks: linux_process_start_ticks(std::process::id()),
            },
            ordinary_usage_allowed: response.ordinary_usage_allowed,
            rate_limits_by_limit_id: BTreeMap::from([(
                "codex".to_string(),
                usage_limit("codex".to_string(), &response.rate_limits),
            )]),
            rate_limit_reset_credits_available: response
                .rate_limit_reset_credits
                .as_ref()
                .map(|summary| summary.available_count),
        };

        write_snapshot(&path, &snapshot, 0).expect("write snapshot");

        let raw = fs::read_to_string(&path).expect("read snapshot");
        let value: Value = serde_json::from_str(&raw).expect("valid json");
        assert_eq!(value["schema"], SNAPSHOT_SCHEMA);
        assert_eq!(value["rate_limit_reset_credits_available"], 1);
        assert_eq!(
            value["rate_limits_by_limit_id"]["codex"]["credits"]["balance"],
            "4632.0"
        );
        assert!(!raw.contains("do-not-persist"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(path).expect("metadata").permissions().mode() & 0o777,
                0o600
            );
        }
    }
}
