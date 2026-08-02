# Z10 typed usability facade

Executable release-gate evidence is recorded in
[`zarr-z10-evidence.md`](zarr-z10-evidence.md).

This note records the first compile-checked public contract for the usability
facade. It is intentionally additive: the descriptor/compiler, primitive block,
region, and provider APIs remain the advanced kernel underneath it.

The high-level boundary has three typed values:

```scala
val spec = value(ArraySpec(DType.Int16, value(Shape(2L, 3L)), value(Shape(2L, 3L))))
val data = value(DenseArray.copyOf(DType.Int16, spec.shape, Array[Short](1, 2, 3, 4, 5, 6)))
```

`DType` is a type-level witness. `ArraySpec[D]` and `DenseArray[D]` carry the
same witness, while rank remains a checked runtime value. `DenseArray.copyOf`
defensively copies ordinary primitive arrays once; an internal owned path is
reserved for adapters that already own their storage. `DenseArray.adopt` is the
explicit public transfer-of-ownership constructor; it does not copy and its
caller must stop mutating the input array immediately after adoption.

The sync and async creation/read façades preserve the same typed values and
errors. `SyncZarr.createArray` and `createAndOpenArray` carry a
`TypedWriteResult`/`TypedCreateAndOpen` so incomplete publication is never
mistaken for success. The async surface remains `Future`-based and requires an
explicit `ExecutionContext`; the sync surface remains blocking by construction.
Neither surface adds a global retry, cache, credential, or scheduler policy.

`SyncZarr` and `AsyncZarr` build direct and indexed-sharding descriptors from
`ArraySpec` without metadata JSON. `ChunkProvider.fromDense` handles border
padding lazily and `ChunkProvider.fill` makes fill-only arrays explicit. The
JVM `JvmZarr` façade accepts only `java.nio.file.Path` and delegates to staged,
atomic publication. `BrowserZarr` delegates to the portable async kernel and
uses the browser codec runtime as its default. These are platform boundaries,
not alternate writer implementations.

The contract intentionally does not infer chunk sizes, compression, retry,
credentials, blocking, or mutation. The default format is v3, direct arrays use
regular chunks with little-endian bytes and zero fill, and indexed sharding or
codecs must be selected explicitly. The raw descriptor and provider APIs remain
the escape hatch for custom or not-yet-typed data types; the README labels that
route as advanced without deprecating it.

The compile contract is exercised in `TypedArraySuite`: it covers scalar,
empty, arbitrary-rank, all fixed-width built-in witnesses, defensive ownership,
spec validation, and a compile-time assignment that must reject an `Int16`
dense value as `Float32`. `ReadmeQuickstartSuite` executes the README's
for-comprehension on JVM and Scala.js; the typed read/write suites cover receipts,
partial regions, sharding, fill omission, async parity, and failure boundaries.
