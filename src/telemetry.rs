//! Executor ops-log helper (mirrors public crate instrumentation).

use photon_telemetry::ops_log;
use serde_json::{json, Value};

const MAX_ERROR_LEN: usize = 512;

fn truncate_error(message: &str) -> String {
    if message.len() <= MAX_ERROR_LEN {
        message.to_string()
    } else {
        let max_prefix_len = MAX_ERROR_LEN.saturating_sub(1);
        let prefix_end = message
            .char_indices()
            .map(|(index, character)| index + character.len_utf8())
            .take_while(|end| *end <= max_prefix_len)
            .last()
            .unwrap_or(0);
        format!("{}…", &message[..prefix_end])
    }
}

fn ops_log_fields(
    component: &str,
    operation: &str,
    message: &str,
    topic: &str,
    subscription: &str,
    error: &str,
) -> Value {
    json!({
        "component": component,
        "operation": operation,
        "message": message,
        "topic": topic,
        "subscription": subscription,
        "error": truncate_error(error),
    })
}

/// Emit a `photon_ops_log` row via the installed [`photon_telemetry::OpsLog`].
pub fn log_ops(
    component: &str,
    operation: &str,
    message: &str,
    topic: &str,
    subscription: &str,
    error: &str,
) {
    ops_log().log_event(
        "photon_ops_log",
        &ops_log_fields(component, operation, message, topic, subscription, error),
    );
}

#[cfg(test)]
mod tests {
    use super::truncate_error;

    #[test]
    fn truncate_error_preserves_utf8_boundaries() {
        let message = "é".repeat(300);

        assert_eq!(truncate_error(&message), format!("{}…", "é".repeat(255)));
    }
}
