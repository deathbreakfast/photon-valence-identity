//! Shared helpers for Photon instrumentation integration tests.
#![allow(missing_docs)]

use photon_telemetry::RecordingOpsLog;

pub fn assert_counter(log: &RecordingOpsLog, name: &str, labels: &[(&str, &str)]) {
    let hits = log.recorded_counters_matching(name, labels);
    assert!(
        !hits.is_empty(),
        "expected counter {name} with labels {labels:?}, got {:?}",
        log.counters()
    );
}

pub fn assert_no_counter(log: &RecordingOpsLog, name: &str, labels: &[(&str, &str)]) {
    let hits = log.recorded_counters_matching(name, labels);
    assert!(
        hits.is_empty(),
        "unexpected counter {name} with labels {labels:?}"
    );
}

pub fn assert_event_field(log: &RecordingOpsLog, event_name: &str, field: &str, expected: &str) {
    let events = log.recorded_events_for(event_name);
    assert!(
        events.iter().any(|e| {
            e.payload
                .get(field)
                .and_then(|v| v.as_str())
                .is_some_and(|s| s == expected)
        }),
        "expected event {event_name}.{field}={expected:?}, got {events:?}",
    );
}
