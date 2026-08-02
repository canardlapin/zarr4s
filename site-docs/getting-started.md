# Getting started

This guide uses the JVM projection and an in-memory store so the first result
has no filesystem, browser, or cloud setup. The same typed construction is
shared by the Scala.js projection; this page's mdoc execution is a JVM check,
not a claim that JVM mdoc alone proves Scala.js behavior.

The public workflow has four steps:

1. construct checked runtime shapes;
2. declare an `ArraySpec` with a dtype and chunk shape;
3. copy values into an owned `DenseArray`; and
4. create, open, and read the array through `SyncZarr`.

The helper below turns an unexpected `Left` into an exception so mdoc stops at
the source line that failed. Application code should normally keep the
`Either[ZarrError, A]` and compose or handle it explicitly.

```scala mdoc:silent
import zarr4s.*

def checked[A](value: Either[ZarrError, A]): A =
  value match
    case Right(result) => result
    case Left(error)   => throw IllegalArgumentException(error.message)

val shape = checked(Shape(2L, 3L))
val chunks = checked(Shape(2L, 3L))
val spec = checked(ArraySpec(DType.Int16, shape, chunks))
val values = checked(
  DenseArray.copyOf(
    DType.Int16,
    shape,
    Array[Short](1, 2, 3, 4, 5, 6)
  )
)
val store = checked(MemoryStore.empty)
val created = checked(SyncZarr.createAndOpenArray(store, spec, values))
val opened = checked(created.opened)
val read = checked(opened.readAll())
```

`ArraySpec` validates the shape and chunk intent. `DenseArray.copyOf` takes
ownership of a copy of the values, so the caller's original array can change
without changing the Zarr input. The typed handle checks the stored dtype
before materializing the result.

```scala mdoc
(read.shape.toVector, read.data.toArray.toVector, read.receipt.bytesRead)
```

The result is a typed read plus an execution receipt:

```text
(Vector(2, 3), Vector(1, 2, 3, 4, 5, 6), 12)
```

The receipt reports the bytes used by this read. It is not a benchmark result:
the example uses an in-memory store and a single uncompressed chunk.

## Keep failures explicit

The library does not install a global throwing, retry, caching, or scheduling
policy. Construction, metadata, storage, codec, and resource-limit failures
are represented as `ZarrError` values. The `checked` helper above is an
example-only boundary where an application chooses to throw; a service can
instead log, retry through its own policy, or return the error to its caller.

## Where to go next

- [The typed workflow](concepts/typed-workflow.md) explains the ownership and
  refinement boundaries.
- [Reading a region](guides/reading-regions.md) adds a bounded selection.
- The repository's [standalone consumer](https://github.com/canardlapin/zarr4s/tree/main/examples/standalone-consumer)
  shows a separate JVM/Scala.js build against the locally published artifact.
