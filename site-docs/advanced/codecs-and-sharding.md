# Configure codecs and sharding

Codecs determine how a logical chunk becomes bytes. Sharding packs multiple
inner chunks into one outer object with an index. Choose them only after the
array shape, access pattern, and target runtimes are known.

## Add a built-in codec

The high-level writer accepts an ordered `Vector[ArrayCodecSpec]`. A direct v3
array supplies little-endian bytes automatically when the vector is empty. If
you supply codecs explicitly, include every stage the format requires.

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
val compressedStore = checked(MemoryStore.empty)
val compressed = checked(
  SyncZarr.createAndOpenArray(
    compressedStore,
    spec,
    values,
    codecs = Vector(ArrayCodecSpec.Bytes.little, ArrayCodecSpec.Gzip(level = 1)),
    runtime = JvmCodecRuntime.portable
  )
)
val compressedRead = checked(checked(compressed.opened).readAll())
```

```scala mdoc
compressedRead.data.toArray.toVector == values.toArray.toVector
```

Metadata compilation and codec execution are separate checks. A known gzip
stage still fails to open if the supplied runtime has no gzip executor. This
is a missing capability, not a reason to reinterpret the bytes.

## Add indexed sharding

The array's `chunkShape` becomes the outer shard shape. `ShardingSpec.indexed`
defines the inner chunk shape and the index profile:

```scala mdoc:silent
val shardedStore = checked(MemoryStore.empty)
val sharding = ShardingSpec.indexed(checked(Shape(2L, 3L)))
val sharded = checked(
  SyncZarr.createAndOpenArray(
    shardedStore,
    spec,
    values,
    sharding = Some(sharding)
  )
)
val shardedRead = checked(checked(sharded.opened).readAll())
```

```scala mdoc
(
  shardedRead.data.toArray.take(4).toVector,
  shardedRead.receipt.touchedShards,
  shardedRead.receipt.touchedChunks
)
```

For this example, each 2 × 3 outer chunk contains one 2 × 3 inner chunk, so
sharding changes the format but gains no packing advantage. In real data, an
outer shard should usually contain several inner chunks. Measure representative
selections: smaller objects can increase request count, while larger shards
can increase transferred bytes for narrow reads.

## Optional Blosc and Zstandard

The separate `zarr4s-codec-blosc-zstd` artifact supplies metadata capabilities
and JVM/Scala.js executors. Opening requires both:

```scala
import zarr4s.codec.blosc.*

val capabilities = BloscZstdProvider.capabilities()
val runtime = JvmBloscZstdRuntime.portable

val opened = SyncZarr.openTypedArray(
  store,
  DType.Float32,
  capabilities = capabilities,
  runtime = runtime
)
```

Use `BrowserBloscZstdRuntime.portable` on Scala.js after installing the pinned
npm dependencies. The Scala.js Blosc encoder currently accepts only the
provider's validated `typesize == 4` contract.

Writing optional Blosc or standalone Zstandard is awkward at the typed façade:
`ArrayCodecSpec` has no external-codec case. A caller must construct or parse
extension metadata, compile an `ArrayDescriptor` with provider capabilities,
and use the advanced writer. A simpler API would let an external provider
contribute a checked programmatic codec specification without reopening the
closed `ArrayCodecSpec` hierarchy.

| Choice | Logical values | Chunk geometry | Stored bytes | Runtime requirement |
| --- | --- | --- | --- | --- |
| Add gzip/zlib/CRC32C | Preserved. | Preserved. | Codec chain changes. | Matching executor. |
| Add transpose | Preserved after decode. | Logical shape preserved; encoded order changes. | Array stage changes. | Built-in array codec support. |
| Add indexed sharding | Preserved. | Adds inner chunks within outer shards. | Object layout and index change. | Sharding and index codecs. |

Next: [choose chunk shapes from measured workloads](choosing-chunks.md).
