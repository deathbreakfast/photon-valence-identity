//! Handler dispatch with backpressure, DLQ, and coalesced checkpoints.

use std::sync::Arc;

use photon_backend::{
    checkpoint::CheckpointCoalescer,
    delivery::{DlqRecordParams, DlqSink, WorkerPool},
    instrumentation,
    models::SubscriptionMode,
    Event,
};
use valence::ValenceFactory;

use crate::handler_descriptor::HandlerDispatch;

/// Dispatch a single event to one handler: build a [`valence::Valence`], acquire a worker-pool
/// permit, run the handler, and record the outcome.
///
/// On success for a durable handler, coalesces a checkpoint update via `coalescer`. On failure —
/// either building the [`valence::Valence`] from `ev.actor_json` or running the handler itself —
/// records the event to `dlq` instead of checkpointing, so a durable subscription never advances
/// past an event its handler could not process.
///
/// Called by [`crate::start_executor`]'s per-topic dispatch loop; not normally called directly by
/// host code.
#[allow(clippy::too_many_arguments)]
pub async fn dispatch_handler(
    valence_factory: Arc<dyn ValenceFactory>,
    pool: Arc<WorkerPool>,
    coalescer: Arc<CheckpointCoalescer>,
    dlq: Arc<DlqSink>,
    subscription_name: Option<&'static str>,
    is_durable: bool,
    topic_name: String,
    dispatch: HandlerDispatch,
    ev: Event,
) {
    let _permit = pool.acquire().await;
    let ev_event_id = ev.event_id.clone();
    let ev_topic_key = ev.topic_key.clone();
    let ev_seq = ev.seq;

    // same shape gate as ValenceIdentityFactory::reconstruct before factory.build.
    if let Err(e) = serde_json::from_value::<valence::Actor>(ev.actor_json.clone()) {
        let _ = dlq.record(&DlqRecordParams {
            event_id: &ev_event_id,
            topic_name: &topic_name,
            topic_key: ev_topic_key.as_deref(),
            seq: ev_seq,
            subscription_name,
            reason: instrumentation::FailureReason::IdentityBuild,
            error: format!("actor_json is not a valid valence::Actor: {e}"),
        });
        return;
    }

    let valence = match valence_factory.build(&ev.actor_json) {
        Ok(v) => v,
        Err(e) => {
            let _ = dlq.record(&DlqRecordParams {
                event_id: &ev_event_id,
                topic_name: &topic_name,
                topic_key: ev_topic_key.as_deref(),
                seq: ev_seq,
                subscription_name,
                reason: instrumentation::FailureReason::IdentityBuild,
                error: e.to_string(),
            });
            return;
        }
    };

    if let Err(e) = spectra_core::worker_scope(dispatch(valence, ev)).await {
        let _ = dlq.record(&DlqRecordParams {
            event_id: &ev_event_id,
            topic_name: &topic_name,
            topic_key: ev_topic_key.as_deref(),
            seq: ev_seq,
            subscription_name,
            reason: instrumentation::FailureReason::HandlerError,
            error: e.to_string(),
        });
        return;
    }

    if is_durable {
        if let Some(name) = subscription_name {
            if let Err(_e) = coalescer
                .record(name, &topic_name, ev_topic_key.as_deref(), ev_seq)
                .await
            {
                instrumentation::record_handler_failure(
                    &topic_name,
                    instrumentation::FailureReason::CheckpointError,
                );
            } else {
                instrumentation::record_drain(&topic_name, name);
            }
        }
    }
}

/// Whether `mode` requires a checkpoint after a successful dispatch.
pub fn is_durable_descriptor(mode: SubscriptionMode) -> bool {
    mode == SubscriptionMode::Durable
}

#[cfg(test)]
mod tests {
    use super::*;
    use photon_backend::models::SubscriptionMode;

    #[test]
    fn is_durable_descriptor_matches_mode() {
        assert!(is_durable_descriptor(SubscriptionMode::Durable));
        assert!(!is_durable_descriptor(SubscriptionMode::Ephemeral));
    }
}
