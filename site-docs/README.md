# zarr4s guide

`zarr4s` is a cross-platform Scala 3 library for creating, reading, and
validating Zarr arrays and groups on the JVM and Scala.js. It keeps dtype,
shape, storage capabilities, and I/O failures explicit at the boundaries where
they matter.

> **Status:** 0.1 pre-release. The guide is compiled from this checkout; no
> stable release artifact is claimed yet.

The ordinary path is a typed dense array:

- describe the dtype, shape, and chunk shape with `ArraySpec`;
- copy ordinary Scala values into an owned `DenseArray`;
- create the array through `SyncZarr` or `AsyncZarr`; and
- read it back through a typed handle with an `ExecutionReceipt`.

Start with [Getting started](getting-started.md), which builds a complete
in-memory array and evaluates the read result with mdoc. Then continue with:

- [The typed workflow](concepts/typed-workflow.md) — what each value means and
  where validation occurs;
- [Reading a region](guides/reading-regions.md) — select a bounded sub-region
  without dropping shape or dtype information; and
- [Build and verification](reference/build.md) — run the Laika/mdoc site and
  distinguish JVM guide coverage from the repository's Scala.js gates.

The low-level descriptor and provider APIs remain available for streaming
inputs, custom storage, and fragment-level control. They are deliberately
introduced after the typed path rather than used as the first example.
