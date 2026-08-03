# Z11 closeout: optional first-party Ravel interoperability

Date: 2026-08-03

Implementation candidate: `dcffada0188ac35c30ebe93b74cf862d1e9f04b9`

Ravel dependency court: `d0f7bacfe3b750519dc49aca8fd466ef70ef24ec`, published only to the local sbt
repository as `0.0.0-d0f7bac`.

## Result

The implementation is accepted as an optional first-party adapter. It does not change zarr4s-core,
replace `DenseArray`, or introduce scientific-domain policy. The production dependency direction is
one-way:

```text
zarr4s-interop-ravel -> zarr4s-core
zarr4s-interop-ravel -> ravel-core
```

The optional Blosc/Zstandard artifact depends on the adapter only in its test configuration. The
site depends on the adapter to compile its executable guide and API reference. Normal publication
of the adapter remains disabled.

## Contract audit

Confirmed:

- Exact native mappings exist for Bool, Int8, UInt8, Int16, UInt16, Int32, Int64, Float32, and
  Float64. Unsupported Zarr dtypes fail through `RavelInteropError.UnsupportedDType`; none widen
  under the native API names.
- Zarr `Long` dimensions and products are checked before conversion to Ravel's portable `Int`
  shape and buffer model. Scalar, empty, and rank-five arrays use the same checked path.
- Reads allocate one owned canonical Ravel destination and preserve the original Zarr execution
  receipt. They do not route through `DenseArray`.
- Canonical immutable Ravel writes retain their source owner and allocate nominal chunk blocks.
  Views require explicit `RavelArraySource.copyOf`; mutable and borrowed arrays are excluded by the
  public signatures.
- Zarr failures remain typed as `RavelInteropError.Zarr`; Ravel shape and non-contiguous-layout
  failures retain their Ravel error values. Materialization failures have a distinct adapter case.
- Sync and async reads, direct and sharded v2/v3 writes, border chunks, missing chunks, limits, and
  conflicts reuse the existing zarr4s descriptor, codec, reader, writer, and receipt contracts.
- Searches of `core` found no Ravel, ScalaFIM, NIfTI, BIDS, calibration, credential, or scheduler
  dependency or policy.

The Scala type-discipline review found no suppressed warnings, unchecked matches, general-purpose
`Any` carriers, sentinel states, or production casts. The two unsigned wrapper constructions use
Ravel's checked-by-construction internal `unsafe` constructors only after masking a decoded UInt8 or
UInt16 primitive to its exact bit width. The sole production catch translates a Ravel
materialization exception into the dedicated typed adapter error.

## Exact-revision verification

An independent clone at the candidate revision was clean before and after the gate. A linked Git
worktree was attempted first, but sbt-git's JGit integration rejected the linked repository as bare
before loading any project task. The independent clone supplied a normal `.git` layout without
weakening the clean-revision court.

The following passed from `/private/tmp/zarr4s-z11-clone-dcffada`:

```text
npm ci --prefix codec-blosc-zstd/js
sbt -Dravel.version=0.0.0-d0f7bac -batch checkAll
```

The full gate covered formatting, every JVM and Scala.js compilation/test aggregate, Scaladoc for
all six APIs, the executable mdoc guide, and the Laika site. Relevant totals included 236 core JVM,
238 core Scala.js, 24 adapter JVM, 23 adapter Scala.js, 23 optional-codec JVM, 21 optional-codec
Scala.js, and 3 benchmark tests. The site rendered 20 HTML pages with zero mdoc errors.

The exact candidate also passed the bidirectional Zarr-Python 3.2.1/NumPy 2.5.1 court, zarrs 0.23.13
reading of adapter v2 output, and adapter reads of the attributed zarrs fixture on both platforms.
The fixture court used `/private/tmp/zarr4s-z11-exact.X8RfDL`. Full commands, fixture hashes, and
qualifications are retained in `docs/benchmarks/zarr-z11-ravel-conformance.md`.

## Allocation and consumer evidence

The refreshed exact-revision allocation receipts support only these claims:

- native Int32 read materialization adds one output-sized Ravel buffer plus small scaffolding;
- canonical source refinement does not copy the payload;
- write production is chunk-bounded rather than array-sized;
- explicit view materialization costs one output-sized copy; and
- the measured `DenseArray` bridge is materially more allocation-heavy for this workload.

The retained JVM and Node.js raw receipts, workload, counter limitations, and timing non-claims are
in `docs/benchmarks/zarr-z11-ravel-allocation.md`. No storage-level zero-copy, universal speedup,
browser-performance, or Scala Native claim is made.

Commit-labelled, unsigned verification artifacts were published locally as
`zarr4s-core`/`zarr4s-interop-ravel` `0.1.0-z11.dcffada`; their POMs pin Ravel
`0.0.0-d0f7bac`. A fresh domain-neutral consumer outside both repositories compiled and ran on JVM
and Scala.js using only those artifacts. Both executions produced `[2, 3, 4, 5, 6, 7]`; the
resolved classpaths contained local artifact jars and no sibling source project. The consumer path,
commands, and hashes are retained in `examples/ravel-standalone-consumer/README.md`.

A temporary ScalaFIM test, injected through sbt without editing its dirty checkout, used the public
adapter to round-trip an Int16 Ravel array. ScalaFIM then applied its own `ScalarCalibration` and
constructed its own typed `ResponseBlock`; the probe passed. This confirms the desired integration
seam while keeping calibration, response schemas, spatial meaning, and other scientific policy
downstream.

## Qualifications and release decision

Qualified:

- Node's array-buffer counter is representation evidence, not a portable heap profiler. The JVM
  thread-allocation counter is authoritative for the chunk budget.
- The local artifact court proves resolvability and binary consumption, but the artifacts are
  unsigned and are not a public release.
- Node.js executes the shared Scala.js adapter and representation paths. No browser performance
  court was run.

Unverified and intentionally unclaimed:

- Scala Native support, cloud-service behavior, compressed end-to-end throughput, and performance
  on workloads other than the retained court.

Public release remains blocked. A live check on 2026-08-03 found no GitHub releases for
`canardlapin/ravel`, and Maven Central returned 404 for
`io/github/canardlapin/ravel-core_3/`. Therefore `interopRavel / publish / skip := true` is the
correct normal build policy. The adapter can be enabled for publication after Ravel has an
immutable published cross-platform artifact and the dependency is changed from the snapshot
default to that release.
