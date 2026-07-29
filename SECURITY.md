# Security Policy

## Supported versions

Security fixes are accepted against the latest `main` branch and tagged releases (`0.1.x`) of this repository's crates (`photon-valence-identity`).

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security-sensitive reports.

Prefer one of the following:

1. **GitHub Security Advisories** — use [Report a vulnerability](https://github.com/unified-field-dev/photon-valence-identity/security/advisories/new) on this repository when available.
2. Contact the maintainers privately via the repository owner listed at https://github.com/unified-field-dev/photon-valence-identity.

Include:

- a description of the issue and its impact
- steps to reproduce or a proof of concept when possible
- affected crate names and versions

We will acknowledge receipt as soon as practical and coordinate a fix and disclosure timeline with you.

## Scope

In scope: vulnerabilities in this repository's published crates and documentation that could cause unsafe production defaults, plus CI/supply-chain issues in this repository.

Out of scope: vulnerabilities solely in third-party dependencies unless this project mishandles them in a security-relevant way.


## Authentication and authorization

This crate reconstructs Valence sessions from captured actor JSON; actor identity and
permission checks belong to Valence (via `DatabaseRouter` and the backend it resolves to).
## Host checklist: publish actor + factory trust

1. **Publish** — derive `actor_json` server-side from the session (never accept client-supplied
   System actors). Photon stores whatever the host passes; this kit cannot rewrite publish.
2. **Dispatch factory** — use [`ProcessValenceFactory::new`] /
   [`router_config_reject_external_system`](src/process_factory.rs) so replay of forged System
   JSON fails closed at `valence_factory.build`.
3. **Background System work** — install
   [`ProcessValenceFactory::arc_internal`](src/process_factory.rs) via
   [`set_process_system_valence_factory`](src/system_valence.rs) for [`system_valence`].
4. **Shape gate** — local executor `dispatch_handler` rejects non-`Actor` JSON before build.
