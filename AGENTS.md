# AGENTS.md

Guidance for coding agents working in **zarr4s**, a Scala 3 Zarr implementation
cross-compiled to the JVM and Scala.js.

## Modules

- `core`: dependency-free metadata, geometry, codec programs, object-store
  capabilities, readers, create-only writers, caches, and platform transports.
- `codec-blosc-zstd`: optional Blosc/Zstandard provider backed by `blosc-java`
  on the JVM and pinned `numcodecs` WebAssembly on Scala.js.

Keep generic Zarr mechanics in this repository. Scientific-domain profiles,
BIDS/NIfTI adapters, credential policy, retention policy, and scheduler
integration belong in downstream libraries.

## Build and test

- Scala: 3.7.4
- Platforms: JVM and Scala.js
- Tests: MUnit
- Full gate: `sbt checkAll`
- Focused gate: `sbt coreJVM/test coreJS/test`
- Optional provider prerequisite:
  `npm ci --prefix codec-blosc-zstd/js`

A change is not complete until the affected JVM and Scala.js suites pass.
Preserve exact external fixtures and their attribution.

## Scala style

Use Scala 3 significant indentation and two-space indents. Prefer precise
algebraic types, smart constructors, explicit capabilities, typed errors, and
owned primitive storage. Keep public values immutable. Confine mutable arrays
and loops to validated numerical or byte-processing internals.

Do not hide retry, concurrency, caching, credentials, or blocking execution in
global state. Callers must supply those policies explicitly.

## Compatibility

Treat the Zarr specification and cross-implementation fixtures as external
contracts. Unsupported metadata must fail through `ZarrError`; do not guess.
Core must not acquire JNI, WebAssembly, cloud SDK, or scientific-domain
dependencies. Optional codecs belong in separate artifacts.

## GitHub identity

The canonical repository is `canardlapin/zarr4s`. Use the repository-local
identity `canardlapin <307091466+canardlapin@users.noreply.github.com>`.
