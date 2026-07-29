//! Host-facing runtime wiring: Photon + handler executor.
//!
//! [`build_photon_runtime`] is the one-call entry point most hosts should use: it pins a
//! process-global [`ValenceFactory`] (see [`crate::system_valence()`]), builds Photon with
//! auto-discovered topics, and starts this crate's [`start_executor`] so `#[photon::subscribe]`
//! handlers begin running immediately.
//!
//! # Examples
//!
//! ```rust,no_run
//! use valence::{install_default_mem_router, RouterValenceFactory, RouterValenceFactoryConfig};
//! use valence::DEFAULT_IN_MEMORY_ROUTER_KEY;
//!
//! # fn main() -> anyhow::Result<()> {
//! let router = install_default_mem_router();
//! let valence_factory = RouterValenceFactory::arc(
//!     router,
//!     RouterValenceFactoryConfig::new(DEFAULT_IN_MEMORY_ROUTER_KEY),
//! );
//! let runtime = photon_valence_identity::build_photon_runtime(&valence_factory)?;
//! // Keep `runtime` alive; dropping `runtime.executor` aborts handler dispatch.
//! # let _ = runtime;
//! # Ok(())
//! # }
//! ```
//!
//! Runnable: `cargo run -p photon-valence-identity --example wire_factory`

use std::sync::Arc;

use photon_runtime::runtime::build_photon_parts;
use valence::ValenceFactory;

use crate::executor::{start_executor, ExecutorHandle};

/// Configured Photon parts (storage port, handler registry, executor services) from upstream
/// `photon-runtime`. Re-exported here so callers of [`build_photon_runtime`] don't need a direct
/// dependency on `photon-runtime` just to name this type.
pub use photon_runtime::runtime::PhotonRuntimeParts;

/// Photon runtime: configured [`photon_runtime::Photon`] plus executor handle.
///
/// Returned by [`build_photon_runtime`]. Keep both fields alive for the life of your process:
/// dropping `executor` aborts all handler dispatch tasks (see [`ExecutorHandle`]).
///
/// # Examples
///
/// ```rust,no_run
/// use valence::{install_default_mem_router, RouterValenceFactory, RouterValenceFactoryConfig};
/// use valence::DEFAULT_IN_MEMORY_ROUTER_KEY;
///
/// # fn main() -> anyhow::Result<()> {
/// let router = install_default_mem_router();
/// let valence_factory = RouterValenceFactory::arc(
///     router,
///     RouterValenceFactoryConfig::new(DEFAULT_IN_MEMORY_ROUTER_KEY),
/// );
/// let runtime = photon_valence_identity::build_photon_runtime(&valence_factory)?;
/// // `runtime.photon` — publish / subscribe
/// // `runtime.executor` — keep alive until shutdown
/// # let _ = runtime;
/// # Ok(())
/// # }
/// ```
pub struct PhotonRuntime {
    /// The configured Photon handle — use it to `publish` events or build typed topic streams.
    pub photon: Arc<photon_runtime::Photon>,
    /// Handle to the running handler executor; abort()s all subscriptions when dropped.
    pub executor: ExecutorHandle,
}

/// Build Photon with auto-discovered topics and start the handler executor.
///
/// Pins `valence_factory` for [`crate::system_valence()`], builds Photon parts, and calls
/// [`start_executor`] so `#[photon::subscribe]` handlers begin dispatching immediately.
///
/// # Examples
///
/// ```rust,no_run
/// use valence::{install_default_mem_router, RouterValenceFactory, RouterValenceFactoryConfig};
/// use valence::DEFAULT_IN_MEMORY_ROUTER_KEY;
///
/// # fn main() -> anyhow::Result<()> {
/// let router = install_default_mem_router();
/// let valence_factory = RouterValenceFactory::arc(
///     router,
///     RouterValenceFactoryConfig::new(DEFAULT_IN_MEMORY_ROUTER_KEY),
/// );
/// let runtime = photon_valence_identity::build_photon_runtime(&valence_factory)?;
/// # let _ = runtime;
/// # Ok(())
/// # }
/// ```
///
/// Prefer [`crate::ProcessValenceFactory::arc`] when you want external System rejection on the
/// dispatch path. See also `cargo run -p photon-valence-identity --example wire_factory`.
///
/// # Errors
///
/// Returns an error if Photon's parts fail to build (e.g. invalid `PHOTON_TRANSPORT_KEY` or
/// storage adapter configuration).
pub fn build_photon_runtime(
    valence_factory: &Arc<dyn ValenceFactory>,
) -> anyhow::Result<PhotonRuntime> {
    crate::system_valence::set_process_valence_factory(Arc::clone(valence_factory));
    let parts = build_photon_parts()?;

    crate::telemetry::log_ops(
        "runtime",
        "init",
        "Photon initialized (mem backend)",
        "",
        "",
        "",
    );

    let executor = start_executor(
        &parts.photon,
        valence_factory,
        &parts.photon.runtime().executor_services,
    );

    Ok(PhotonRuntime {
        photon: parts.photon,
        executor,
    })
}
