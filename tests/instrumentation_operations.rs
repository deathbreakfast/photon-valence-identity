//! Per-operation Photon instrumentation tests (in-process `RecordingOpsLog`).
//!
//! Test-only topic/handler fixtures below are intentionally undocumented; this binary target is
//! exempt from the library's `missing_docs = "deny"` lint (see `Cargo.toml`).
#![allow(missing_docs)]

mod common;
mod instrumentation_support;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use photon::{configure, Photon};
use photon_telemetry::{install_ops_log, NoOpsLog, RecordingOpsLog};
use serde_json::json;
use serial_test::serial;
use tokio::time::Duration;

use instrumentation_support::{assert_counter, assert_event_field, assert_no_counter};

fn install_ops_recorder() -> RecordingOpsLog {
    common::ensure_photon_test_env();
    let log = RecordingOpsLog::new();
    install_ops_log(Arc::new(log.clone()));
    log
}

struct FailValenceFactory;

impl valence::ValenceFactory for FailValenceFactory {
    fn build(&self, _actor_json: &serde_json::Value) -> valence::Result<valence::Valence> {
        Err(valence::Error::Identity(
            "valence build failed for test".into(),
        ))
    }
}

#[photon::topic(name = "test.instr.publish")]
pub struct InstrPublishEvent {
    pub n: u32,
}

#[photon::topic(name = "test.instr.durable")]
pub struct InstrDurableEvent {
    pub n: u32,
}

static DURABLE_OK: AtomicU32 = AtomicU32::new(0);

#[photon::subscribe(topic = "test.instr.durable", durable = "test.instr.durable.sub")]
#[allow(clippy::unused_async)]
pub async fn on_instr_durable(
    actor: Box<dyn photon::Actor>,
    _ev: InstrDurableEvent,
) -> photon::Result<()> {
    let _ = actor;
    DURABLE_OK.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[photon::topic(
    name = "test.instr.ephemeral",
    delivery = "group",
    shards = 4,
    shard_by = "partition_key"
)]
pub struct InstrEphemeralEvent {
    pub partition_key: String,
    pub n: u32,
}

static EPHEMERAL_OK: AtomicU32 = AtomicU32::new(0);

#[photon::subscribe(topic = "test.instr.ephemeral", group = "test.instr.ephemeral.grp")]
#[allow(clippy::unused_async)]
pub async fn on_instr_ephemeral(
    actor: Box<dyn photon::Actor>,
    _ev: InstrEphemeralEvent,
) -> photon::Result<()> {
    let _ = actor;
    EPHEMERAL_OK.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[photon::topic(name = "test.instr.fail")]
pub struct InstrFailEvent;

#[photon::subscribe(topic = "test.instr.fail", durable = "test.instr.fail.sub")]
#[allow(clippy::unused_async)]
pub async fn on_instr_fail(
    actor: Box<dyn photon::Actor>,
    _ev: InstrFailEvent,
) -> photon::Result<()> {
    let _ = actor;
    Err(photon::PhotonError::Internal(
        "handler failed on purpose".into(),
    ))
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn publish_local_emits_photon_publishes() {
    let sink = install_ops_recorder();
    let photon = Photon::builder().auto_registry().build().expect("build");

    let topic = "test.instr.publish";
    let _ = photon
        .publish(
            topic,
            None,
            json!({"System": {"operation": "t"}}),
            json!({"n": 1}),
        )
        .await
        .expect("publish");

    assert_counter(
        &sink,
        "photon_publishes",
        &[("topic", topic), ("backend", "mem")],
    );
    assert_no_counter(&sink, "photon_drains", &[("topic", topic)]);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn publish_mem_backend_emits_photon_publishes() {
    let sink = install_ops_recorder();
    let photon = Photon::builder().auto_registry().build().expect("build");

    let topic = "test.instr.publish.dist";
    let _ = photon
        .publish(
            topic,
            None,
            json!({"System": {"operation": "t"}}),
            json!({"n": 1}),
        )
        .await
        .expect("publish");

    assert_counter(
        &sink,
        "photon_publishes",
        &[("topic", topic), ("backend", "mem")],
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn durable_handler_checkpoint_emits_drain() {
    let _sink = install_ops_recorder();
    DURABLE_OK.store(0, Ordering::SeqCst);

    let photon = Arc::new(Photon::builder().auto_registry().build().expect("build"));
    configure((*photon).clone());
    common::start_photon_executor(photon.as_ref(), common::TestValenceFactory::arc())
        .expect("start executor");
    tokio::time::sleep(Duration::from_millis(200)).await;

    InstrDurableEvent { n: 1 }.publish().await.expect("publish");

    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if DURABLE_OK.load(Ordering::SeqCst) >= 1 {
            break;
        }
    }
    assert!(
        DURABLE_OK.load(Ordering::SeqCst) >= 1,
        "durable handler should run under upstream Photon executor"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn durable_handler_error_emits_dlq_not_drain() {
    let sink = install_ops_recorder();

    let photon = Arc::new(Photon::builder().auto_registry().build().expect("build"));
    configure((*photon).clone());
    common::start_photon_executor(photon.as_ref(), common::TestValenceFactory::arc())
        .expect("start executor");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let topic = "test.instr.fail";
    photon
        .publish(
            topic,
            None,
            json!({"System": {"operation": "t"}}),
            json!({}),
        )
        .await
        .expect("publish");

    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if !sink.recorded_events_for("photon_dlq").is_empty() {
            break;
        }
    }
    assert_event_field(&sink, "photon_dlq", "reason", "handler_error");
    assert_counter(
        &sink,
        "photon_handler_failures",
        &[("topic", topic), ("reason", "handler_error")],
    );
    assert_no_counter(
        &sink,
        "photon_drains",
        &[("topic", topic), ("subscription", "test.instr.fail.sub")],
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn valence_build_fail_emits_dlq_not_drain() {
    let sink = install_ops_recorder();

    let photon = Arc::new(Photon::builder().auto_registry().build().expect("build"));
    configure((*photon).clone());
    common::start_photon_executor(
        photon.as_ref(),
        Arc::new(FailValenceFactory) as Arc<dyn valence::ValenceFactory>,
    )
    .expect("start executor");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let topic = "test.instr.fail";
    photon
        .publish(
            topic,
            None,
            json!({"System": {"operation": "t"}}),
            json!({}),
        )
        .await
        .expect("publish");

    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if !sink.recorded_events_for("photon_dlq").is_empty() {
            break;
        }
    }
    assert_event_field(&sink, "photon_dlq", "reason", "identity_build");
    assert!(
        sink.recorded_events_for("photon_dlq").iter().any(|e| e
            .payload
            .get("error")
            .and_then(|v| v.as_str())
            .is_some_and(|s| s.contains("valence build failed for test"))),
        "DLQ error should mention valence build failure"
    );
    assert_no_counter(&sink, "photon_drains", &[("topic", topic)]);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn ephemeral_handler_ok_no_drain() {
    let sink = install_ops_recorder();
    EPHEMERAL_OK.store(0, Ordering::SeqCst);

    std::env::set_var("PHOTON_GROUP_SHARD_COUNT", "4");
    std::env::set_var("PHOTON_GROUP_SHARD_ASSIGNMENT", "0-3");

    let photon = Arc::new(Photon::builder().auto_registry().build().expect("build"));
    configure((*photon).clone());
    common::start_photon_executor(photon.as_ref(), common::TestValenceFactory::arc())
        .expect("start executor");
    tokio::time::sleep(Duration::from_millis(200)).await;

    InstrEphemeralEvent {
        partition_key: "pk-0".into(),
        n: 1,
    }
    .publish()
    .await
    .expect("publish");

    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if EPHEMERAL_OK.load(Ordering::SeqCst) >= 1 {
            break;
        }
    }
    assert!(EPHEMERAL_OK.load(Ordering::SeqCst) >= 1);
    assert_no_counter(&sink, "photon_drains", &[("topic", "test.instr.ephemeral")]);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn distinct_publish_and_drain_labels() {
    let sink = install_ops_recorder();
    DURABLE_OK.store(0, Ordering::SeqCst);

    let photon = Arc::new(Photon::builder().auto_registry().build().expect("build"));
    configure((*photon).clone());
    common::start_photon_executor(photon.as_ref(), common::TestValenceFactory::arc())
        .expect("start executor");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let topic = "test.instr.durable";
    InstrDurableEvent { n: 1 }.publish().await.expect("publish");

    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let publishes = !sink
            .recorded_counters_matching("photon_publishes", &[("topic", topic)])
            .is_empty();
        if DURABLE_OK.load(Ordering::SeqCst) >= 1 && publishes {
            break;
        }
    }

    assert_counter(&sink, "photon_publishes", &[("topic", topic)]);
    assert!(
        DURABLE_OK.load(Ordering::SeqCst) >= 1,
        "durable handler should run under upstream Photon executor"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn unset_ops_log_no_panic() {
    common::ensure_photon_test_env();
    install_ops_log(Arc::new(NoOpsLog));
    let photon = Photon::builder().build().expect("build");
    let _ = photon
        .publish(
            "test.instr.nosink",
            None,
            json!({"System": {"operation": "t"}}),
            json!({}),
        )
        .await
        .expect("publish");
}
