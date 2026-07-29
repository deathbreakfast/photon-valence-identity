//! Persist actor JSON, reconstruct Photon identity / Valence for a worker path.
//!
//! ## When to use
//! Show how Photon handler identity survives publish → worker via captured actor JSON.
//!
//! ## Command
//! ```bash
//! CARGO_BUILD_JOBS=1 cargo run --example persist_actor_recover
//! ```
//!
//! ## Success
//! Stderr prints `persist_actor_recover: OK — actor persisted + Photon identity recovered`.
//!
//! ## See also
//! [`ValenceIdentityFactory`](photon_valence_identity::ValenceIdentityFactory),
//! `wire_factory`.

#![allow(clippy::print_stderr, clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;
use std::sync::Arc;

use photon_core::IdentityFactory;
use photon_valence_identity::{
    set_process_system_valence_factory, Actor, ProcessValenceFactory, ValenceIdentityFactory,
};
use valence::{install_default_mem_router, DEFAULT_IN_MEMORY_ROUTER_KEY};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let router = install_default_mem_router();
    let external = ProcessValenceFactory::arc(Arc::clone(&router), DEFAULT_IN_MEMORY_ROUTER_KEY);
    let internal = ProcessValenceFactory::arc_internal(router, DEFAULT_IN_MEMORY_ROUTER_KEY);
    set_process_system_valence_factory(internal);

    let user = Actor::User {
        user_id: "persist-user".into(),
    };
    let user_json = serde_json::to_value(&user)?;

    let path: PathBuf = std::env::temp_dir().join("photon-valence-identity-actor.json");
    std::fs::write(&path, serde_json::to_vec_pretty(&user_json)?)?;

    let loaded: serde_json::Value = serde_json::from_slice(&std::fs::read(&path)?)?;
    let valence = external.build(&loaded)?;
    assert_eq!(valence.actor().user_id(), Some("persist-user"));

    let identity_factory = ValenceIdentityFactory::new(Arc::clone(&external));
    let reconstructed = identity_factory.reconstruct(&serde_json::to_string(&user)?)?;
    let _ = reconstructed.label();

    let _ = std::fs::remove_file(&path);
    eprintln!("persist_actor_recover: OK — actor persisted + Photon identity recovered");
    Ok(())
}
