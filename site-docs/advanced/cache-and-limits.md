# Control remote I/O with caches and limits

Remote reads can transfer more bytes than the requested logical values. Use
execution receipts to observe that cost, a revision-scoped cache to reuse
immutable objects and ranges, and explicit limits to bound work before tuning
for speed.

## Cache one immutable revision

A cache namespace identifies the exact immutable store revision. Reusing a
namespace after objects change can return stale bytes, so the library requires
the caller to name the revision rather than guessing one globally.

```scala mdoc:silent
import zarr4s.*

def checked[A](value: Either[ZarrError, A]): A =
  value.fold(error => throw IllegalArgumentException(error.message), identity)

val shape = checked(Shape(4L, 6L))
val chunks = checked(Shape(2L, 3L))
val spec = checked(ArraySpec(DType.Int16, shape, chunks))
val values = checked(
  DenseArray.copyOf(DType.Int16, shape, (1 to 24).map(_.toShort).toArray)
)
val sourceStore = checked(MemoryStore.empty)
val write = checked(SyncZarr.createArray(sourceStore, spec, values))
val _ = checked(write.outcome.toEither)

val namespace = checked(CacheNamespace.from("measurements-revision-42"))
val cache = ObjectReadCache(namespace)
val cachedStore = CachingObjectReader(sourceStore, cache)
val opened = checked(SyncZarr.openTypedArray(cachedStore, DType.Int16))

val first = checked(opened.readAll())
val downstreamAfterFirst = cache.stats.downstreamRequests
val second = checked(opened.readAll())
val downstreamAfterSecond = cache.stats.downstreamRequests
```

```scala mdoc
(
  first.data.toArray.toVector == second.data.toArray.toVector,
  downstreamAfterSecond == downstreamAfterFirst,
  cache.stats.hits,
  cache.stats.residentEntries
)
```

The cache stores whole objects, exact ranges, and lengths. A containing range
or whole object may satisfy a smaller range request. The asynchronous decorator
also collapses identical in-flight reads. Store errors and failed futures are
not retained.

## Read the receipt before tuning

`ExecutionReceipt` contains physical request counts and byte totals, logical
element counts, and touched chunks or shards. Its `readAmplification` is:

```text
physical encoded bytes read / requested logical scalar bytes
```

It is not elapsed time and it is not a benchmark. Compression may produce an
amplification below 1.0; a narrow selection from a large uncompressed chunk may
produce a value far above 1.0.

## Bound opening, reading, and writing separately

| Limits | Applies to | Examples of bounded work |
| --- | --- | --- |
| `OpenLimits` | Metadata and descriptor opening | Metadata bytes, rank, decoded chunk size, hierarchy discovery. |
| `ReadLimits` | One read operation | Objects, ranges, concurrent requests, encoded object size, decoding, shard indexes, planning. |
| `WriterLimits` | One create-only publication | Objects, chunks, encoded chunk or shard bytes, metadata, total written bytes. |
| `CacheLimits` | One revision cache | Resident entries and bytes. |

```scala mdoc:compile-only
import zarr4s.*

val conservativeReads = ReadLimits(
  maxObjects = 10_000,
  maxRanges = 50_000,
  maxConcurrentRequests = 4
)

val smallCache = CacheLimits(
  maxEntries = 512,
  maxBytes = checked(ByteCount(64L * 1024L * 1024L))
)
```

Limit constructors use `require` for invalid configuration such as negative
counts. Limit exceedance during a Zarr operation is returned as
`ZarrError.ResourceLimit`.

Caching is explicit decoration: `SyncZarr` and `AsyncZarr` do not mutate a
global cache, and they do not choose when a remote revision has changed.

Next: [check the exact supported format surface](../reference/support.md).
