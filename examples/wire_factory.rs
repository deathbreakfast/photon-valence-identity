//! Minimal in-memory wiring for Photon Valence identity.
//!
//! Uses [`ProcessValenceFactory`] (external System reject) for reconstruct, and
//! [`ProcessValenceFactory::arc_internal`] for background System sessions.

#![allow(clippy::print_stderr)]

use std::sync::Arc;

use photon_core::IdentityFactory;
use photon_valence_identity::{
    set_process_system_valence_factory, system_valence, Actor, ProcessValenceFactory,
    ValenceIdentityFactory,
};
use valence::{install_default_mem_router, DEFAULT_IN_MEMORY_ROUTER_KEY};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let router = install_default_mem_router();
    let external = ProcessValenceFactory::arc(Arc::clone(&router), DEFAULT_IN_MEMORY_ROUTER_KEY);
    let internal = ProcessValenceFactory::arc_internal(router, DEFAULT_IN_MEMORY_ROUTER_KEY);
    set_process_system_valence_factory(internal);

    let user = Actor::User {
        user_id: "wire-factory-user".into(),
    };
    let user_json = serde_json::to_value(&user)?;
    let valence = external.build(&user_json)?;
    let _ = valence.database_router();

    let identity_factory = ValenceIdentityFactory::new(Arc::clone(&external));
    let reconstructed = identity_factory.reconstruct(&serde_json::to_string(&user)?)?;
    let _ = reconstructed.label();

    let system = Actor::System {
        operation: "wire_factory".into(),
    };
    match external.build(&serde_json::to_value(&system)?) {
        Ok(_) => return Err("external factory must reject System actor JSON".into()),
        Err(e) => eprintln!("wire_factory: System rejected on external path — {e}"),
    }

    let _ = system_valence("wire_factory_bg")?;

    eprintln!("wire_factory: OK — User reconstruct + System reject + system_valence");
    Ok(())
}
