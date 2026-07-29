# photon-valence-identity

[![CI](https://github.com/unified-field-dev/photon-valence-identity/actions/workflows/ci.yml/badge.svg)](https://github.com/unified-field-dev/photon-valence-identity/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

[GitHub](https://github.com/unified-field-dev/photon-valence-identity) · `cargo doc -p photon-valence-identity --open`

Valence-backed `IdentityFactory` and runtime helpers for [Photon](https://github.com/unified-field-dev/photon) handlers.

```toml
photon-valence-identity = { git = "https://github.com/unified-field-dev/photon-valence-identity" }
```

`build_photon_runtime` pins your [Valence](https://github.com/unified-field-dev/valence)
`ValenceFactory`, builds Photon with auto-discovered topics, and starts the handler executor —
one call, no separate `start_executor` step required:

```rust
use photon_valence_identity::build_photon_runtime;

// `valence_factory: Arc<dyn valence::ValenceFactory>`, e.g. from `RouterValenceFactory::arc(...)`.
let runtime = build_photon_runtime(&valence_factory)?;
// `runtime.photon` publishes/subscribes; `runtime.executor` dispatches
// `#[photon::subscribe]` handlers until dropped.
```

Full Getting started (including `ValenceIdentityFactory` with upstream Photon's own executor)
lives in `cargo doc --open`.

## What it provides

- `ValenceIdentityFactory` / `ProcessValenceFactory` — reconstruct Photon actors through Valence
- Handler registry, dispatch, and executor wiring
- `build_photon_runtime` helpers for hosts that opt into Valence-backed identity

## Examples

Canonical teaching path and run commands: [examples/README.md](examples/README.md).

## Verify

```bash
export CARGO_BUILD_JOBS=1
cargo test
```

## License

MIT. See [LICENSE](LICENSE), [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md), and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).
