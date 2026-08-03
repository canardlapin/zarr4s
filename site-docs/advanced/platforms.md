# Choose a platform boundary

Use the synchronous API when the caller already has synchronous object
capabilities and can control blocking. Use the asynchronous API when reads and
writes should compose as `Future`s. `BrowserZarr` is the Scala.js-oriented
asynchronous façade with browser codec defaults.

The array, metadata, selection, error, and receipt models are shared. The
effect and store capabilities change.

| Boundary | Store capability | Result | Default codec runtime |
| --- | --- | --- | --- |
| `SyncZarr` | `ObjectReader` / `ObjectWriter` | `Either[ZarrError, A]` | Dependency-free shared core |
| `AsyncZarr` | `AsyncObjectReader` / `AsyncObjectWriter` | `Future[Either[ZarrError, A]]` | Dependency-free shared core |
| `JvmZarr` | New filesystem `Path` for staged create-only publication | `Either[ZarrError, A]` | JVM gzip and zlib included |
| `BrowserZarr` | Async browser-compatible store | `Future[Either[ZarrError, A]]` | Browser gzip and zlib included |

## Compose the asynchronous API

`AsyncZarr` requires an explicit `ExecutionContext`. The following uses the
asynchronous in-memory store so the example is deterministic; a remote store
has the same result shape.

```scala mdoc:silent
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
import zarr4s.*

given ExecutionContext = ExecutionContext.parasitic

def checked[A](value: Either[ZarrError, A]): A =
  value.fold(error => throw IllegalArgumentException(error.message), identity)

val shape = checked(Shape(4L, 6L))
val chunks = checked(Shape(2L, 3L))
val spec = checked(ArraySpec(DType.Int16, shape, chunks))
val values = checked(
  DenseArray.copyOf(DType.Int16, shape, (1 to 24).map(_.toShort).toArray)
)
val store = checked(AsyncMemoryStore(Map.empty))

val result = AsyncZarr
  .createAndOpenArray(store, spec, values)
  .flatMap:
    case Left(error) => scala.concurrent.Future.successful(Left(error))
    case Right(created) =>
      created.opened match
        case Left(error)   => scala.concurrent.Future.successful(Left(error))
        case Right(opened) => opened.readAll()

val read = checked(Await.result(result, 2.seconds))
```

```scala mdoc
(read.shape.toVector, read.data.toArray.takeRight(3).toVector)
```

Do not use `Await` in browser or non-blocking application code. It appears
only so the JVM documentation process can evaluate the `Future`.

## Adapt blocking JVM work deliberately

`JvmHttpStore` is synchronous. To use it behind `AsyncZarr`, wrap it with the
provided blocking adapters and supply an execution context reserved for
blocking work. Do not use a general compute pool for unbounded network or file
blocking.

`ReadLimits.maxConcurrentRequests` bounds portable asynchronous read
scheduling. The asynchronous writer deliberately processes one provider,
codec, or store effect at a time; it does not claim hidden parallel writes.

## Browser differences

`FetchStore` validates range responses and bounds response bodies. It does not
provide listing, credentials, retry, or persistent caching. Configure browser
authentication through the surrounding application and supply a compatible
store when Fetch's default request construction is insufficient.

The site’s mdoc project depends on `coreJVM`. JVM-executed examples therefore
prove shared syntax and JVM behavior, not Scala.js behavior. The repository's
`coreJS/test` and optional-provider Scala.js suites are the cross-platform
evidence.

Next: [configure codecs and sharding](codecs-and-sharding.md).
