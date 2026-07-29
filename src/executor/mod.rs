//! Local executor that dispatches events from the bus to registered handlers.
//!
//! [`start_executor`] is this module's entry point: it discovers every `#[photon::subscribe]`
//! handler via [`HandlerRegistry::auto_discover`], opens one Photon subscription per distinct
//! `(topic, key filter)` pair, and spawns a dispatch task per matching event. Per-event dispatch
//! (backpressure, DLQ recording, checkpoint coalescing) lives in [`dispatch_handler`]; computing
//! where a durable subscription should resume lives in [`initial_subscription_after_seq`].
//!
//! # Examples
//!
//! ```rust,ignore
//! use photon_valence_identity::start_executor;
//!
//! # fn boot(
//! #     photon: &std::sync::Arc<photon_runtime::Photon>,
//! #     valence_factory: &std::sync::Arc<dyn valence::ValenceFactory>,
//! #     services: &std::sync::Arc<photon_backend::ExecutorServices>,
//! # ) {
//! let handle = start_executor(photon, valence_factory, services);
//! // Dropping `handle` (or calling `handle.abort()`) stops all handler dispatch.
//! # let _ = handle;
//! # }
//! ```
//!
//! Most hosts should prefer [`crate::build_photon_runtime`], which calls this for you.

mod dispatch;
mod initial_seq;

use std::sync::Arc;

use futures::StreamExt;
use photon_backend::ExecutorServices;
use photon_runtime::Photon;
use tokio::task::JoinHandle;
use valence::ValenceFactory;

use crate::handler_registry::HandlerRegistry;

pub use dispatch::{dispatch_handler, is_durable_descriptor};
pub use initial_seq::initial_subscription_after_seq;

/// Handle to the running executor; abort all subscription tasks when dropped.
///
/// Returned by [`start_executor`]. Drop it (or call [`ExecutorHandle::abort`] explicitly) to stop
/// dispatching events to `#[photon::subscribe]` handlers.
///
/// # Examples
///
/// ```rust
/// use photon_valence_identity::ExecutorHandle;
///
/// let handle = ExecutorHandle::empty();
/// handle.abort(); // idempotent noop when no tasks were spawned
/// ```
pub struct ExecutorHandle {
    join_handles: Vec<JoinHandle<()>>,
}

impl ExecutorHandle {
    /// A handle with no subscription tasks — what [`start_executor`] returns when no
    /// `#[photon::subscribe]` handlers were discovered.
    pub const fn empty() -> Self {
        Self {
            join_handles: Vec::new(),
        }
    }

    /// Abort every subscription task. Safe to call multiple times.
    pub fn abort(&self) {
        for h in &self.join_handles {
            h.abort();
        }
    }
}

impl Drop for ExecutorHandle {
    fn drop(&mut self) {
        self.abort();
    }
}

/// Start the executor for all registered handler subscriptions.
///
/// Discovers every `#[photon::subscribe]` handler via [`HandlerRegistry::auto_discover`], groups
/// them by `(topic_name, topic_key_filter)`, and spawns one subscription task per group. Each
/// task replays from the group's minimum durable checkpoint (see
/// [`initial_subscription_after_seq`]) and dispatches matching events through
/// [`dispatch_handler`], building a fresh [`valence::Valence`] per event from `valence_factory`.
///
/// Returns [`ExecutorHandle::empty`] immediately if no handlers are registered. Most hosts should
/// prefer [`crate::build_photon_runtime`], which calls this for you.
///
/// # Examples
///
/// ```rust,ignore
/// use photon_valence_identity::start_executor;
///
/// # fn boot(
/// #     photon: &std::sync::Arc<photon_runtime::Photon>,
/// #     valence_factory: &std::sync::Arc<dyn valence::ValenceFactory>,
/// #     services: &std::sync::Arc<photon_backend::ExecutorServices>,
/// # ) {
/// // `photon`, `valence_factory`, and `services` typically come from
/// // `photon_runtime::runtime::build_photon_parts`.
/// let handle = start_executor(photon, valence_factory, services);
/// // Dropping `handle` (or calling `handle.abort()`) stops all handler dispatch.
/// # let _ = handle;
/// # }
/// ```
pub fn start_executor(
    photon: &Arc<Photon>,
    valence_factory: &Arc<dyn ValenceFactory>,
    services: &Arc<ExecutorServices>,
) -> ExecutorHandle {
    let registry = HandlerRegistry::auto_discover();
    if registry.is_empty() {
        return ExecutorHandle::empty();
    }

    let keys = registry.topic_subscription_keys();
    let mut join_handles = Vec::with_capacity(keys.len());

    for (topic_name, topic_key_filter) in keys {
        let photon_clone = Arc::clone(photon);
        let services_clone = Arc::clone(services);
        let valence_factory_clone = Arc::clone(valence_factory);
        let registry_ref = HandlerRegistry::auto_discover();
        let handlers =
            registry_ref.handlers_for_topic_key(&topic_name, topic_key_filter.as_deref());
        if handlers.is_empty() {
            continue;
        }

        let join = tokio::spawn(async move {
            let after_seq = initial_subscription_after_seq(
                photon_clone.as_ref(),
                &topic_name,
                topic_key_filter.as_deref(),
                &handlers,
            )
            .await;
            let mut stream =
                photon_clone.subscribe(&topic_name, topic_key_filter.as_deref(), after_seq);
            while let Some(result) = stream.next().await {
                let event = match result {
                    Ok(ev) => ev,
                    Err(e) => {
                        crate::telemetry::log_ops(
                            "executor",
                            "stream_error",
                            "executor stream error",
                            &topic_name,
                            "",
                            &e.to_string(),
                        );
                        continue;
                    }
                };
                for descriptor in &handlers {
                    if !descriptor.matches_event(&event.topic_name, event.topic_key.as_deref()) {
                        continue;
                    }
                    let valence_factory_inner = Arc::clone(&valence_factory_clone);
                    let services_inner = Arc::clone(&services_clone);
                    let ev = event.clone();
                    let dispatch = descriptor.dispatch;
                    let subscription_name = descriptor.subscription_name;
                    let topic = event.topic_name.clone();
                    let is_durable = is_durable_descriptor(descriptor.mode);
                    tokio::spawn(async move {
                        dispatch_handler(
                            valence_factory_inner,
                            Arc::clone(&services_inner.worker_pool),
                            Arc::clone(&services_inner.checkpoint_coalescer),
                            Arc::clone(&services_inner.dlq),
                            subscription_name,
                            is_durable,
                            topic,
                            dispatch,
                            ev,
                        )
                        .await;
                    });
                }
            }
        });
        join_handles.push(join);
    }

    ExecutorHandle { join_handles }
}

#[cfg(test)]
mod tests {
    use super::ExecutorHandle;

    #[test]
    fn empty_handle_abort_is_noop() {
        let handle = ExecutorHandle::empty();
        handle.abort();
        handle.abort();
    }
}
