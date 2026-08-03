# User-guide editorial review

Date: 2026-08-03

Scope: README, public guide, source tree, public API, JVM and Scala.js tests,
standalone consumer, optional codec module, design and benchmark records,
module/build topology, workflows, and recent architectural history through
`215d265`.

This is an editorial and API-clarity review, not a new format-conformance or
release certification. Current verification results for the revised guide are
recorded in the final change handoff rather than inferred from historical
evidence notes.

## Editorial assessment

`zarr4s` has a stronger contract than its original four-page guide exposed.
The repository consistently treats shape, dtype, ownership, partial write
progress, codec availability, store capabilities, and resource limits as
observable values. Tests cover the same shared mechanisms on the JVM and
Scala.js and include attributed independent fixtures. This is credible raw
material for a mature guide.

The original site was accurate and executable but too small to be the primary
documentation for the library. It taught one in-memory create/read workflow
and one rectangular selection. Installation status, existing-store workflows,
point and factored selections, groups, create-only outcomes, platforms, codecs,
sharding, caches, limits, support boundaries, and troubleshooting remained in
the README, tests, or source.

### Evidence classification

| Claim | Classification | Basis |
| --- | --- | --- |
| The typed dense path is the intended ordinary entry point. | Confirmed | README, `TypedArray`, typed read/write façades, quick-start compile gate, and typed suites agree. |
| Shared core targets JVM and Scala.js. | Confirmed | Cross-project build and corresponding test trees. |
| JVM mdoc examples prove Scala.js behavior. | Rejected | The site depends on `coreJVM`; Scala.js requires its own suites. |
| Common v2/v3, selection, codec, and sharding claims have executable repository evidence. | Confirmed within the recorded support matrix | Shared/platform suites and independent fixtures exist for the named surface. This review did not rerun every external oracle. |
| A stable artifact is available. | Unverified and currently contradicted by repository state | No release tag or stable coordinate is declared. |
| The guide and API reference are built as one deployable site. | Confirmed locally; publication pending | `docsBundle` builds the mdoc/Laika guide plus four module/platform Scaladoc trees and the site links them explicitly. |
| Historical benchmark values are general performance guarantees. | Rejected | Benchmark records state their fixtures and non-claims. |

## Strengths to preserve

1. **Truthful failure behavior.** Unsupported required metadata returns
   `ZarrError`; the implementation does not substitute an approximate codec,
   dtype, or layout.
2. **Explicit ownership.** `DenseArray.copyOf`, `adopt`, and defensive result
   copies make mutation boundaries reviewable.
3. **Create-only publication.** `WriteOutcome` retains progress when a generic
   store cannot roll back, and filesystem publication has a stronger staged
   boundary.
4. **Capability separation.** Metadata compilation, execution runtimes,
   storage, listing, caching, and concurrency are not process-global policy.
5. **Cross-platform discipline.** Shared semantics are tested on both targets;
   platform transports and optional native/Wasm dependencies stay outside the
   dependency-free core.
6. **Evidence discipline.** Independent fixture provenance, bounded failure
   cases, and explicit non-claims are part of the repository rather than
   marketing copy.
7. **Public/internal documentation boundary.** `site-docs/` contains public
   guidance; `docs/` retains design, benchmark, and acceptance records without
   publishing them as a random user-site hierarchy.

## Documentation weaknesses found

1. The original guide had no installation page. A first-time reader could not
   tell whether a Maven coordinate existed or how to use a checkout lawfully.
2. The first workflow created an in-memory array. Opening an existing file,
   HTTP store, or browser store—often the first real task—was absent.
3. The mental model named typed façade values but did not first connect logical
   arrays, chunk grids, stored objects, codecs, and receipts.
4. Region reading was documented in isolation. Point order, duplicates,
   factored Cartesian selection, output shapes, and read amplification were
   undiscoverable.
5. Create-only conflicts and incomplete progress were described in prose but
   not shown as a complete handling workflow.
6. Groups, consolidation modes, and listing requirements were absent.
7. Built-in and optional codec setup, sharding consequences, cache revision
   identity, and resource-limit families were absent.
8. The contributor build page occupied the only Reference section. User
   support and API wayfinding were missing.
9. No troubleshooting page mapped observable symptoms to causes and remedies.
10. Important operations did not summarize which values, shape, dtype,
    metadata, objects, and receipts change.

## Implemented documentation architecture

```text
Overview
Start
  Installation status
  First array
Learn
  Arrays, chunks, and typed values
  Errors and ownership
Use
  Open existing arrays
  Select data
  Write arrays safely
  Navigate groups
Advanced
  Choose a platform boundary
  Configure codecs and sharding
  Choose chunk shapes
  Decide whether to use shards
  Tune remote reads and caches
  Control remote I/O with caches and limits
Reference
  Supported formats and data
  Public API map
Help
  Troubleshooting
Project
  Build and verify
```

The order follows prerequisite and task rather than package layout. One 4 × 6
`int16` measurement array continues from first use through selection, writing,
groups, platform effects, encoding, caching, and receipts. JVM-compatible
examples are compiled or evaluated with mdoc. Scala.js-only examples are
identified as such and depend on the repository's Scala.js gates for proof.

## API issues exposed by the rewrite

### 1. Simplify the ordinary completed-write result

`createAndOpenArray` returns an outer `Either`, then
`TypedCreateAndOpen.opened` contains another `Either`, while publication status
also lives in `WriteOutcome`. The states are meaningful, but the routine path
requires nested failure handling.

Recommendation: keep the current detailed result for recovery-sensitive code,
and add a convenience method whose success value can exist only after complete
publication and successful reopen. Its single `Left` should retain a distinct
error case carrying partial `WriteProgress` when publication began.

### 2. Finish using one error vocabulary for store construction

`JvmFileStore.openChecked` now returns `Either[ZarrError, JvmFileStore]`, which
keeps JVM path opening inside the ordinary typed workflow. The older
`JvmFileStore.open`, `JvmHttpStore.apply`, and `FetchStore.apply` constructors
still return `Either[String, Store]`.

Recommendation: introduce a typed store-construction error or return
`Either[ZarrError, Store]` consistently.

### 3. JVM open façades beside JVM create façades — addressed

`JvmZarr` now exposes `openArray`, `openTypedArray`, `openGroup`, and `openNode`
for `Path`. The methods validate the path through `JvmFileStore.openChecked`
and delegate to the shared synchronous façade.

The store APIs remain available for callers that need reuse, caching, or custom
composition.

### 4. Group creation in the ordinary façade — addressed

Immutable `GroupSpec` and `GroupWriteResult` now give group creation the same
format, attributes, path, limits, and outcome vocabulary as typed array
creation. `SyncZarr`, `AsyncZarr`, and `JvmZarr` expose the operation directly;
the advanced writers remain available.

This removes the documentation-only detour through raw `GroupMetadata`.

### 5. Open programmatic codec configuration to providers

`ArrayCodecSpec` is a closed hierarchy for built-in codecs. The optional Blosc
and Zstandard provider can compile and execute external metadata, but the typed
writer cannot express those codecs without dropping to descriptor/metadata
construction.

Recommendation: define a checked provider-owned codec-spec interface that
produces extension metadata and declares its capability name. Preserve codec
program validation in core.

### 6. Add checked metadata builders

Dimension names have a checked typed helper, but attributes require the
library's raw `JsonObject`/`JsonValue` vocabulary. `JsonObject.from` also
returns `Either[String, JsonObject]`, creating another error translation.

Recommendation: add small checked attribute builders for common Scala scalar,
array, and object values, with a consistent typed construction error.

### 7. Validate codec parameters at construction

Built-in `ArrayCodecSpec.Gzip`, `Zlib`, `Shuffle`, and `Transpose` accept raw
parameters and defer validation to descriptor compilation. Optional providers
already use smart constructors for levels and strides.

Recommendation: give built-in programmatic specs the same checked-constructor
discipline while retaining parsing-time validation for external metadata.

## Final assessment

If this repository were announced to the Typelevel audience tomorrow, the
revised guide would make the design and ordinary workflows understandable. The
remaining documentation barriers to a polished, mature impression would be:

1. no resolvable stable artifact and therefore no honest one-line installation;
2. publication of the already linked guide and module/platform Scaladoc bundle;
3. no versioned documentation or migration policy for a public release line;
4. optional-provider authoring that cannot stay in the typed codec-spec API;
   and
5. built-in codec parameters and attribute construction that do not yet share
   one checked-constructor vocabulary.

The performance guide now provides a versioned, executable request-and-byte
court for choosing chunks, shards, and revision caches, while stating that a
real remote endpoint is still required for latency and throughput evidence.
The first two remaining items are release-documentation blockers. The others
are API-coherence issues that documentation can expose but should not disguise.
