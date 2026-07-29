//! Reconstructs Valence sessions for [Photon] handlers and runs the local executor.
//!
//! [Photon]'s handler boundary is deliberately identity-agnostic: it captures an opaque
//! `actor_json` blob at publish time and, when a `#[photon::subscribe]` handler runs, asks a
//! host-supplied [`photon_core::IdentityFactory`] to turn that blob back into a
//! [`photon_core::Actor`]. This crate is that factory for hosts that already use
//! [Valence] as their permission-checked data-access layer: it reconstructs a
//! [`Valence`] session from the captured actor and hands it to your handler as a live,
//! scoped-database handle instead of a bag of claims you have to re-resolve yourself.
//!
//! On top of the identity factory, this crate also ships a small local **executor** —
//! discover `#[photon::subscribe]` handlers via [Quark] inventory, subscribe to their topics on
//! [Photon], and dispatch each event through a [`Valence`] built from that event's actor —
//! plus the [`HandlerRegistry`] plumbing the executor is built on.
//!
//! Runnable: `cargo run -p photon-valence-identity --example wire_factory`
//!
//! [Photon]: https://github.com/unified-field-dev/photon
//! [Valence]: https://github.com/unified-field-dev/valence
//! [Quark]: https://github.com/unified-field-dev/quark
//!
//! ## Features
//!
//! - **Identity factory** — [`ValenceIdentityFactory`] implements Photon's
//!   [`photon_core::IdentityFactory`] from a [`ValenceFactory`], so `#[photon::subscribe]`
//!   handlers can take a [`Valence`] parameter instead of a raw [`photon_core::Actor`].
//! - **Process-global router factory** — [`ProcessValenceFactory`] wraps a pinned
//!   [`valence::DatabaseRouter`] behind [`valence::RouterValenceFactory`], for hosts that keep one
//!   router alive for the life of the process.
//! - **Handler discovery and dispatch** — [`HandlerRegistry`] auto-discovers
//!   `#[photon::subscribe]` handlers via [`inventory`] and [`start_executor`] runs them with
//!   backpressure, dead-letter queueing, and coalesced durable checkpoints.
//! - **One-call runtime wiring** — [`build_photon_runtime`] builds Photon with auto-discovered
//!   topics, pins the process [`ValenceFactory`], and starts the executor in one step.
//! - **System-scoped access** — [`system_valence()`] builds a [`Valence`] for background work
//!   that runs outside any single subscribed event (retention sweeps, startup jobs, …).
//!
//! ## Concern → API
//!
//! | Concern | API |
//! |---------|-----|
//! | Runtime: wire Photon + identity + executor in one call | [`build_photon_runtime`], [`PhotonRuntime`] |
//! | Identity-only: implement Photon's `IdentityFactory` from an existing [`ValenceFactory`] (bring your own Photon / executor) | [`ValenceIdentityFactory`] |
//! | Identity-only: pin one process-global [`valence::DatabaseRouter`] behind a [`ValenceFactory`] | [`ProcessValenceFactory`] |
//! | Executor: discover `#[photon::subscribe]` handlers and dispatch them | [`start_executor`], [`ExecutorHandle`], [`HandlerRegistry`] |
//! | System valence: scoped access for work outside any subscribed event | [`system_valence()`], [`set_process_valence_factory`] |
//!
//! Also see: [`HandlerDescriptor`], [`HandlerDispatch`] — inventory metadata from
//! `#[photon::subscribe]`; [`ExecutorServices`] — worker pool / DLQ / checkpoint services under
//! [`start_executor`] (normally from Photon parts, not constructed by hand).
//!
//! Runnable deep dive: `cargo run -p photon-valence-identity --example wire_factory`
//!
//! # Getting started
//!
//! Most hosts only need [`build_photon_runtime`]: give it a [`ValenceFactory`] and it wires
//! Photon, pins that factory for [`system_valence()`], and starts the handler executor.
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
//!
//! let runtime = photon_valence_identity::build_photon_runtime(&valence_factory)?;
//! // `runtime.photon` publishes/subscribes; `runtime.executor` dispatches
//! // `#[photon::subscribe]` handlers until dropped.
//! # let _ = runtime;
//! # Ok(())
//! # }
//! ```
//!
//! Hosts that call upstream `Photon::start_executor` directly (e.g. because they need Photon's
//! own inventory executor rather than this crate's) can use [`ValenceIdentityFactory`] as the
//! [`photon_core::IdentityFactory`] instead:
//!
//! ```rust,ignore
//! use std::sync::Arc;
//!
//! use photon::Photon;
//! use photon_valence_identity::ValenceIdentityFactory;
//!
//! # fn boot(photon: &Photon, valence_factory: Arc<dyn valence::ValenceFactory>) -> photon::Result<()> {
//! photon.start_executor(Arc::new(ValenceIdentityFactory::new(valence_factory)))
//! # }
//! ```
//!
//! ## Where to look next
//!
//! - [`identity`] — [`ValenceIdentityFactory`], Photon's `IdentityFactory` over a [`ValenceFactory`]
//! - [`process_factory`] — [`ProcessValenceFactory`], the process-global router wrapper
//! - [`runtime`] — [`build_photon_runtime`] and the [`PhotonRuntime`] it returns
//! - [`executor`] — [`start_executor`] and the dispatch loop it spawns
//! - [`mod@system_valence`] — [`system_valence()`], [`set_process_valence_factory`] for background work
//! - [`HandlerRegistry`] — the inventory-backed handler table the executor dispatches from

pub mod executor;
mod handler_descriptor;
mod handler_registry;
pub mod identity;
pub mod process_factory;
pub mod runtime;
pub mod system_valence;
mod telemetry;

pub use executor::{start_executor, ExecutorHandle};
pub use handler_descriptor::{HandlerDescriptor, HandlerDispatch};
pub use handler_registry::HandlerRegistry;
pub use identity::ValenceIdentityFactory;
/// Backend-owned worker pool, DLQ, and checkpoint services, re-exported for callers that
/// build their own executor loop instead of using [`start_executor`].
pub use photon_backend::ExecutorServices;
pub use process_factory::{router_config_reject_external_system, ProcessValenceFactory};
/// Quark inventory, re-exported so downstream crates can register `#[photon::subscribe]`
/// handlers without depending on `uf-quark` directly.
pub use quark::inventory;
pub use runtime::{build_photon_runtime, PhotonRuntime, PhotonRuntimeParts};
pub use system_valence::{
    set_process_system_valence_factory, set_process_valence_factory, system_valence,
};
/// Core Valence types re-exported for convenience: an [`Actor`] identifies who is acting, a
/// [`Valence`] is the permission-checked session handlers receive, and [`ValenceFactory`]
/// reconstructs that session from captured actor JSON.
pub use valence::{Actor, Valence, ValenceFactory};
