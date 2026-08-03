# Navigate groups

A Zarr group gives related arrays and groups stable paths in one store. Open a
known child directly when its path is known. Ask for `children` only when the
store has consolidated metadata or a listing capability.

Create a group through the same façade used for arrays. `GroupSpec` contains
only metadata a creator can choose; parser-only unknown fields do not leak into
the creation API.

```scala mdoc:silent
import zarr4s.*

def checked[A](value: Either[ZarrError, A]): A =
  value.fold(error => throw IllegalArgumentException(error.message), identity)

val store = checked(MemoryStore.empty)
val groupWrite = SyncZarr.createGroup(store, GroupSpec())
val _ = checked(groupWrite.outcome.toEither)

val shape = checked(Shape(4L, 6L))
val chunks = checked(Shape(2L, 3L))
val spec = checked(ArraySpec(DType.Int16, shape, chunks))
val values = checked(
  DenseArray.copyOf(DType.Int16, shape, (1 to 24).map(_.toShort).toArray)
)
val measurementPath = checked(ZarrPath("measurements"))
val measurementWrite = checked(
  SyncZarr.createArray(store, spec, values, path = measurementPath)
)
val _ = checked(measurementWrite.outcome.toEither)
```

`MemoryStore` also implements `ObjectLister`, so an unconsolidated group can
discover its children:

```scala mdoc:silent
val root = checked(SyncZarr.openGroup(store, lister = Some(store)))
val children = checked(root.children)
val measurements = checked(root.openArray("measurements"))
val typedMeasurements = checked(measurements.asTyped(DType.Int16))
```

```scala mdoc
(
  children.map(entry => entry.path.value -> entry.kind.toString),
  checked(typedMeasurements.readAll()).data.toArray.take(3).toVector
)
```

## Choose a consolidation mode

| Mode | Behavior |
| --- | --- |
| `ConsolidationMode.Prefer` | Use supported consolidated metadata when present; otherwise open explicit paths and use a lister for discovery. This is the default. |
| `ConsolidationMode.Require` | Fail if consolidated metadata is absent or does not contain a requested child. |
| `ConsolidationMode.Ignore` | Ignore consolidation and read node metadata directly. Discovery still needs a lister. |

HTTP and Fetch stores do not list objects. They can open explicit child paths,
and they can discover children from supported consolidated metadata, but an
unconsolidated remote hierarchy needs an application-supplied `ObjectLister`
or `AsyncObjectLister`.

| Group operation | Values | Path | Metadata | Store requirement |
| --- | --- | --- | --- | --- |
| `open("child")` | Not materialized. | Resolves below the opened group. | Returns array or group dynamically. | Metadata reads; consolidation may satisfy them. |
| `openArray("child")` | Not materialized. | Resolves below the opened group. | Requires the child to be an array. | Same as `open`. |
| `children` | No array values. | Returns immediate child paths. | Returns kind and format. | Consolidated index or listing capability. |

Use `AsyncZarr.createGroup` with an `AsyncObjectWriter`. On the JVM,
`JvmZarr.createGroup(path, spec)` stages the complete group directory and
publishes it with the same atomic move used for filesystem arrays.

Next: [choose the synchronous, asynchronous, or browser boundary](../advanced/platforms.md).
