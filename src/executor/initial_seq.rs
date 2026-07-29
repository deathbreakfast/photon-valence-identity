//! Compute `after_seq` for subscription streams from durable checkpoints.

use photon_backend::models::SubscriptionMode;
use photon_runtime::Photon;

use crate::handler_descriptor::HandlerDescriptor;

/// Compute the `after_seq` to resume a subscription stream from, based on `handlers`' durable
/// checkpoints.
///
/// Returns:
/// - `None` if no handler in `handlers` is durable (ephemeral streams always start from "now").
/// - `None` if any durable handler is missing a checkpoint (e.g. first run, or the checkpoint
///   lookup failed) — the stream then starts from "now" rather than risking a gap.
/// - `Some(seq)` — the minimum checkpointed sequence across all durable handlers, so no durable
///   handler misses events it hasn't yet processed.
///
/// Called by [`crate::start_executor`] once per `(topic_name, topic_key_filter)` group before
/// opening its subscription stream.
pub async fn initial_subscription_after_seq(
    photon: &Photon,
    topic_name: &str,
    topic_key_filter: Option<&str>,
    handlers: &[&'static HandlerDescriptor],
) -> Option<i64> {
    let mut min_cp: Option<i64> = None;
    let mut any_durable = false;
    let mut all_have_cp = true;

    for h in handlers {
        if h.mode != SubscriptionMode::Durable {
            continue;
        }
        any_durable = true;
        let Some(name) = h.subscription_name else {
            all_have_cp = false;
            continue;
        };
        match photon
            .get_checkpoint_seq(name, topic_name, topic_key_filter)
            .await
        {
            Ok(Some(s)) => {
                min_cp = Some(min_cp.map_or(s, |m: i64| m.min(s)));
            }
            Ok(None) | Err(_) => {
                all_have_cp = false;
            }
        }
    }

    if !any_durable {
        None
    } else if all_have_cp {
        min_cp
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler_descriptor::{HandlerDescriptor, HandlerDispatch};
    use photon_backend::{Event, Result};
    use std::future::Future;
    use std::pin::Pin;

    fn noop_dispatch(
        _: valence::Valence,
        _: Event,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
        Box::pin(async { Ok(()) })
    }

    fn ensure_photon_test_env() {
        std::env::set_var(
            "PHOTON_TRANSPORT_KEY",
            "cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=",
        );
    }

    #[tokio::test]
    async fn ephemeral_only_handlers_resume_from_none() {
        ensure_photon_test_env();
        let photon = Photon::builder().build().expect("build photon");
        let descriptor = Box::leak(Box::new(HandlerDescriptor::new(
            "test.initial.seq.ephemeral",
            None,
            None,
            SubscriptionMode::Ephemeral,
            noop_dispatch as HandlerDispatch,
        )));
        let after = initial_subscription_after_seq(
            &photon,
            "test.initial.seq.ephemeral",
            None,
            &[descriptor],
        )
        .await;
        assert_eq!(after, None);
    }

    #[tokio::test]
    async fn durable_without_checkpoint_resumes_from_none() {
        ensure_photon_test_env();
        let photon = Photon::builder().build().expect("build photon");
        let descriptor = Box::leak(Box::new(HandlerDescriptor::new(
            "test.initial.seq.durable",
            Some("test.initial.seq.durable.sub"),
            None,
            SubscriptionMode::Durable,
            noop_dispatch as HandlerDispatch,
        )));
        let after = initial_subscription_after_seq(
            &photon,
            "test.initial.seq.durable",
            None,
            &[descriptor],
        )
        .await;
        assert_eq!(
            after, None,
            "missing checkpoint must not invent an after_seq"
        );
    }
}
