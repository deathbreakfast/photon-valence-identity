//! System-scoped [`Valence`] for work outside any subscribed event.
//!
//! `#[photon::subscribe]` handlers receive their [`Valence`] from the event that triggered them
//! (via [`crate::executor::dispatch_handler`]). Background work that runs *outside* any single
//! event — retention sweeps, startup jobs, admin tasks — has no event to derive an actor from, so
//! it uses [`system_valence()`] instead, scoped to [`valence::Actor::System`].
//!
//! Prefer installing an **internal-trust** factory via [`set_process_system_valence_factory`]
//! (see [`crate::ProcessValenceFactory::arc_internal`]) so the dispatch factory can keep
//! [`RejectExternalSystemActor`](valence::RejectExternalSystemActor) enabled.
//!
//! # Examples
//!
//! ```rust,no_run
//! use photon_valence_identity::{
//!     set_process_system_valence_factory, system_valence, ProcessValenceFactory,
//! };
//! use valence::{install_default_mem_router, DEFAULT_IN_MEMORY_ROUTER_KEY};
//!
//! # fn main() -> anyhow::Result<()> {
//! let router = install_default_mem_router();
//! set_process_system_valence_factory(ProcessValenceFactory::arc_internal(
//!     router,
//!     DEFAULT_IN_MEMORY_ROUTER_KEY,
//! ));
//! let valence = system_valence("retention_sweep")?;
//! # let _ = valence;
//! # Ok(())
//! # }
//! ```
//!
//! Runnable: `cargo run -p photon-valence-identity --example wire_factory`

use std::sync::{Arc, OnceLock};

use valence::{Actor, Valence, ValenceFactory};

static PROCESS_VALENCE_FACTORY: OnceLock<Arc<dyn ValenceFactory>> = OnceLock::new();
static PROCESS_SYSTEM_VALENCE_FACTORY: OnceLock<Arc<dyn ValenceFactory>> = OnceLock::new();

/// Pin the process [`ValenceFactory`] used by handler dispatch / [`crate::build_photon_runtime`].
///
/// Called automatically by [`crate::build_photon_runtime`]; call this directly only if you build
/// Photon's parts yourself and skip that helper.
///
/// # Examples
///
/// ```rust,no_run
/// use photon_valence_identity::{set_process_valence_factory, ProcessValenceFactory};
/// use valence::{install_default_mem_router, DEFAULT_IN_MEMORY_ROUTER_KEY};
///
/// let router = install_default_mem_router();
/// let factory = ProcessValenceFactory::arc(router, DEFAULT_IN_MEMORY_ROUTER_KEY);
/// set_process_valence_factory(factory);
/// ```
pub fn set_process_valence_factory(factory: Arc<dyn ValenceFactory>) {
    let _ = PROCESS_VALENCE_FACTORY.set(factory);
}

/// Pin the factory used by [`system_valence()`] (should allow System / `ActorTrust::Internal`).
///
/// # Examples
///
/// ```rust,no_run
/// use photon_valence_identity::{
///     set_process_system_valence_factory, ProcessValenceFactory,
/// };
/// use valence::{install_default_mem_router, DEFAULT_IN_MEMORY_ROUTER_KEY};
///
/// let router = install_default_mem_router();
/// set_process_system_valence_factory(ProcessValenceFactory::arc_internal(
///     router,
///     DEFAULT_IN_MEMORY_ROUTER_KEY,
/// ));
/// ```
pub fn set_process_system_valence_factory(factory: Arc<dyn ValenceFactory>) {
    let _ = PROCESS_SYSTEM_VALENCE_FACTORY.set(factory);
}

/// Build a system-scoped [`Valence`] for inventory / background handlers.
///
/// `operation` becomes the label on the [`Actor::System`] used to build the session — keep it
/// short and specific (e.g. `"retention_sweep"`) since it shows up in Valence audit logs.
///
/// # Errors
///
/// Returns an error if no [`ValenceFactory`] has been installed yet (call
/// [`crate::build_photon_runtime`] or [`set_process_valence_factory`] /
/// [`set_process_system_valence_factory`] first), or if the installed factory fails to build a
/// session for the synthetic system actor.
///
/// # Examples
///
/// ```rust,no_run
/// use photon_valence_identity::{
///     set_process_system_valence_factory, system_valence, ProcessValenceFactory,
/// };
/// use valence::{install_default_mem_router, DEFAULT_IN_MEMORY_ROUTER_KEY};
///
/// # fn main() -> anyhow::Result<()> {
/// let router = install_default_mem_router();
/// set_process_system_valence_factory(ProcessValenceFactory::arc_internal(
///     router,
///     DEFAULT_IN_MEMORY_ROUTER_KEY,
/// ));
/// let valence = system_valence("retention_sweep")?;
/// # let _ = valence;
/// # Ok(())
/// # }
/// ```
///
/// See also `cargo run -p photon-valence-identity --example wire_factory`.
pub fn system_valence(operation: &str) -> anyhow::Result<Valence> {
    let factory = PROCESS_SYSTEM_VALENCE_FACTORY
        .get()
        .or_else(|| PROCESS_VALENCE_FACTORY.get())
        .ok_or_else(|| {
            anyhow::anyhow!("ValenceFactory not installed (call build_photon_runtime first)")
        })?;
    let actor = Actor::System {
        operation: operation.to_string(),
    };
    let actor_json = serde_json::to_value(&actor)?;
    factory
        .build(&actor_json)
        .map_err(|e| anyhow::anyhow!("{e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProcessValenceFactory;
    use valence::{install_default_mem_router, DEFAULT_IN_MEMORY_ROUTER_KEY};

    #[test]
    fn system_valence_requires_install_then_succeeds() {
        // Single ordered test: OnceLock can only be set once per process, so unset + install
        // must live in the same test body (and nowhere else in this crate's unit tests).
        let err = system_valence("unit_test").expect_err("factory not installed");
        assert!(
            err.to_string().contains("not installed"),
            "unexpected error: {err}"
        );

        let router = install_default_mem_router();
        set_process_system_valence_factory(ProcessValenceFactory::arc_internal(
            router,
            DEFAULT_IN_MEMORY_ROUTER_KEY,
        ));
        let valence = system_valence("unit_test_ok").expect("system valence");
        let _ = valence;
    }
}
