//! Shared test harness for photon-valence integration tests.
//!
//! This module is compiled into each integration test binary via `mod common;`; its items are
//! test-only fixtures and are exempt from `missing_docs` via `#![allow(missing_docs)]` on those
//! binaries' crate roots.

use std::sync::Arc;

use valence::{
    install_default_mem_router, RouterValenceFactory, RouterValenceFactoryConfig, ValenceFactory,
    DEFAULT_IN_MEMORY_ROUTER_KEY,
};

/// Set the Photon transport key required by recent uf-photon builds.
pub fn ensure_photon_test_env() {
    std::env::set_var(
        "PHOTON_TRANSPORT_KEY",
        "cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=",
    );
}

/// In-memory [`ValenceFactory`] backed by a fresh default-mem router, for tests.
pub struct TestValenceFactory {
    inner: Arc<dyn ValenceFactory>,
}

impl TestValenceFactory {
    /// Build a factory backed by a new in-memory router.
    pub fn new() -> Self {
        let router = install_default_mem_router();
        Self {
            inner: RouterValenceFactory::arc(
                router,
                RouterValenceFactoryConfig::new(DEFAULT_IN_MEMORY_ROUTER_KEY),
            ),
        }
    }

    /// [`TestValenceFactory::new`], boxed as a [`ValenceFactory`] for dependency injection.
    pub fn arc() -> Arc<dyn ValenceFactory> {
        Arc::new(Self::new())
    }
}

impl ValenceFactory for TestValenceFactory {
    fn build(&self, actor_json: &serde_json::Value) -> valence::Result<valence::Valence> {
        self.inner.build(actor_json)
    }
}

/// Start upstream Photon's inventory executor with a Valence identity factory.
pub fn start_photon_executor(
    photon: &photon::Photon,
    factory: Arc<dyn ValenceFactory>,
) -> photon::Result<()> {
    photon.start_executor(Arc::new(
        photon_valence_identity::ValenceIdentityFactory::new(factory),
    ))
}

/// Build this crate's [`PhotonRuntime`](photon_valence_identity::PhotonRuntime) with a
/// [`TestValenceFactory`], panicking on failure.
#[allow(dead_code)]
pub fn test_photon_runtime() -> photon_valence_identity::PhotonRuntime {
    ensure_photon_test_env();
    let factory = TestValenceFactory::arc();
    let Ok(runtime) = photon_valence_identity::build_photon_runtime(&factory) else {
        panic!("build photon runtime");
    };
    runtime
}

/// [`test_photon_runtime`]'s Photon handle, discarding the executor handle.
#[allow(dead_code)]
pub fn test_photon() -> Arc<photon::Photon> {
    test_photon_runtime().photon
}
