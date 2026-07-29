//! Handler descriptor for subscription handler auto-registration.
//!
//! Used by the `#[photon::subscribe]` macro to register handlers at compile time.
//! Dispatch is type-erased so the executor can invoke any handler with (Valence, Event).

use std::future::Future;
use std::pin::Pin;

use photon_backend::{Event, Result, SubscriptionMode};
use valence::Valence;

/// Type-erased dispatch: takes Valence and Event, returns Result.
pub type HandlerDispatch = fn(Valence, Event) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>;

/// Descriptor for a registered subscription handler (from `#[photon::subscribe]` macro).
///
/// Normally created by the `#[photon::subscribe]` macro expansion and submitted to
/// [`quark::inventory`] automatically — see [`HandlerDescriptor::new`] only if you are
/// registering a handler by hand (e.g. in tests, as in this module's `tests` submodule).
#[derive(Clone)]
pub struct HandlerDescriptor {
    /// Topic name this handler subscribes to.
    pub topic_name: &'static str,
    /// Subscription name (required for durable mode).
    pub subscription_name: Option<&'static str>,
    /// Key filter; only events matching this key are dispatched.
    pub topic_key_filter: Option<&'static str>,
    /// Durable or ephemeral.
    pub mode: SubscriptionMode,
    /// Type-erased dispatch: deserializes payload and calls the user's async fn.
    pub dispatch: HandlerDispatch,
}

impl HandlerDescriptor {
    /// Create a new handler descriptor.
    pub const fn new(
        topic_name: &'static str,
        subscription_name: Option<&'static str>,
        topic_key_filter: Option<&'static str>,
        mode: SubscriptionMode,
        dispatch: HandlerDispatch,
    ) -> Self {
        Self {
            topic_name,
            subscription_name,
            topic_key_filter,
            mode,
            dispatch,
        }
    }

    /// Check if this handler matches an event (topic and key filter).
    pub fn matches_event(&self, topic_name: &str, topic_key: Option<&str>) -> bool {
        if self.topic_name != topic_name {
            return false;
        }
        match (&self.topic_key_filter, topic_key) {
            (None, _) => true,
            (Some(fk), Some(ek)) => *fk == ek,
            (Some(_), None) => false,
        }
    }
}

impl std::fmt::Debug for HandlerDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandlerDescriptor")
            .field("topic_name", &self.topic_name)
            .field("subscription_name", &self.subscription_name)
            .field("topic_key_filter", &self.topic_key_filter)
            .field("mode", &self.mode)
            .field("dispatch", &"<fn>")
            .finish()
    }
}

quark::inventory::collect!(HandlerDescriptor);

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;

    fn noop_dispatch(
        _: valence::Valence,
        _: Event,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
        Box::pin(async { Ok(()) })
    }

    #[test]
    fn matches_event_topic_mismatch() {
        let d = HandlerDescriptor::new(
            "topic.a",
            None,
            None,
            SubscriptionMode::Ephemeral,
            noop_dispatch,
        );
        assert!(!d.matches_event("topic.b", None));
    }

    #[test]
    fn matches_event_no_key_filter() {
        let d = HandlerDescriptor::new(
            "topic.a",
            None,
            None,
            SubscriptionMode::Ephemeral,
            noop_dispatch,
        );
        assert!(d.matches_event("topic.a", None));
        assert!(d.matches_event("topic.a", Some("key1")));
    }

    #[test]
    fn matches_event_with_key_filter() {
        let d = HandlerDescriptor::new(
            "topic.a",
            None,
            Some("user-1"),
            SubscriptionMode::Ephemeral,
            noop_dispatch,
        );
        assert!(!d.matches_event("topic.a", None));
        assert!(d.matches_event("topic.a", Some("user-1")));
        assert!(!d.matches_event("topic.a", Some("user-2")));
    }
}
