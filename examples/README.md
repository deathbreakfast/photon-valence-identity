# photon-valence-identity examples

Canonical teaching path for Valence-backed Photon identity — in-memory router;
examples reconstruct actors and build `system_valence`.

## `wire_factory` — external vs internal factory paths

Run when you want to confirm `ValenceIdentityFactory` reconstructs User actors, rejects
external System JSON, and `system_valence` works on the internal factory path.

```bash
cargo run -p photon-valence-identity --example wire_factory
```

Success: stderr prints `wire_factory: System rejected on external path — …` (expected) and
`wire_factory: OK — User reconstruct + System reject + system_valence`.

## `persist_actor_recover` — file-persisted actor JSON → Photon identity

```bash
CARGO_BUILD_JOBS=1 cargo run --example persist_actor_recover
```

Success: stderr prints `persist_actor_recover: OK — actor persisted + Photon identity recovered`.

See `examples/wire_factory.rs`, then pass `ValenceIdentityFactory` into the executor or use
`build_photon_runtime` for the all-in-one host path.
