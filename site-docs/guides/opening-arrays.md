# Open an existing array

Opening has two stages. First choose an object reader for the storage system.
Then ask `SyncZarr`, `AsyncZarr`, or `BrowserZarr` to parse metadata and refine
the array against the dtype your program expects.

The typed open checks metadata and dtype before fetching a data chunk:

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
val store = checked(MemoryStore.empty)
val _ = checked(SyncZarr.createArray(store, spec, values))

val measurements = checked(SyncZarr.openTypedArray(store, DType.Int16))
```

```scala mdoc
(
  measurements.descriptor.shape.toVector,
  measurements.dtype.name,
  measurements.format.toString
)
```

`openTypedArray` is appropriate when the application already knows the dtype.
Use `openArray` when the dtype is genuinely dynamic, inspect
`opened.descriptor.dataType`, and call `opened.asTyped(dtype)` only after the
application has selected a supported witness.

## Open a filesystem store on the JVM

`JvmZarr` accepts an existing directory containing the Zarr root. It opens the
filesystem store, supplies its bounded listing capability for group discovery,
and returns errors through `ZarrError`.

```scala mdoc:compile-only
import java.nio.file.Path
import zarr4s.*

val openedFromDisk: Either[ZarrError, TypedOpenedArray[DType.Int16.type]] =
  JvmZarr.openTypedArray(Path.of("data/measurements.zarr"), DType.Int16)
```

Use `JvmZarr.openNode`, `openArray`, or `openGroup` when the expected node kind
differs. The underlying store refuses keys and symlinks that escape its root.
These calls are synchronous and may block; place them on an execution boundary
appropriate for your application. `JvmFileStore.openChecked` remains available
when an application needs the store as an explicit capability.

## Open an HTTP store on the JVM

`JvmHttpStore` maps Zarr object keys below one base URI. The server must honor
HTTP range requests for partial reads and return a usable `Content-Length` for
operations that need object length.

```scala mdoc:compile-only
import java.net.URI
import zarr4s.*

val openedOverHttp: Either[String, TypedOpenedArray[DType.Int16.type]] =
  for
    httpStore <- JvmHttpStore(URI.create("https://example.org/measurements.zarr/"))
    array <- SyncZarr
      .openTypedArray(httpStore, DType.Int16)
      .left
      .map(_.message)
  yield array
```

`JvmHttpStore` does not provide listing. Explicit paths still work. Group
discovery requires consolidated metadata or a separate `ObjectLister`.

## Open from a browser

On Scala.js, `FetchStore(baseUrl)` provides asynchronous whole-object, range,
and length reads through Fetch. Pass it to `BrowserZarr.openTypedArray`. The
browser façade supplies browser gzip and zlib executors by default and returns
`Future[Either[ZarrError, BrowserTypedOpenedArray[D]]]`.

```scala
given scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

val opened = FetchStore("https://example.org/measurements.zarr/") match
  case Left(detail) =>
    scala.concurrent.Future.successful(Left(detail))
  case Right(store) =>
    BrowserZarr
      .openTypedArray(store, DType.Int16)
      .map(_.left.map(_.message))
```

This snippet is Scala.js-only and is not executed by the JVM mdoc build. In
real code, translate the constructor's string error into the application's
error vocabulary at this boundary.

| Open operation | Data values | Metadata | Store traffic |
| --- | --- | --- | --- |
| `openArray` | Not materialized. | Parsed into a dynamic descriptor. | Metadata objects only. |
| `openTypedArray` | Not materialized. | Parsed and checked against one `DType`. | Metadata objects only. |
| `readAll` or a selection | Materialized into owned storage. | Handle remains unchanged. | Selected chunks or shard ranges plus any required indexes. |

Next: [select only the data you need](selecting-data.md).
