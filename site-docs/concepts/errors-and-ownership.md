# Errors and ownership

Most `zarr4s` boundaries return `Either[ZarrError, A]`. The error identifies
which contract failed: geometry, metadata, dtype refinement, codec execution,
storage, unsupported behavior, or a configured resource limit. The library
does not throw for these expected failures or retry them globally.

```scala mdoc:silent
import zarr4s.*

val wrongLength = for
  shape <- Shape(2L, 2L)
  dense <- DenseArray.copyOf(DType.Int16, shape, Array[Short](1, 2, 3))
yield dense
```

```scala mdoc
wrongLength.left.map(_.message)
```

The failure occurs before a store is touched.

```scala mdoc:silent
def checked[A](value: Either[ZarrError, A]): A =
  value.fold(error => throw IllegalArgumentException(error.message), identity)
```

## Validation happens at the narrowest boundary

| Boundary | Checks before proceeding | Typical error |
| --- | --- | --- |
| `Shape`, `Coordinate`, `Region`, selection constructors | Dimensions, rank, bounds, overflow, and positive slice steps. | `InvalidShape`, `RankMismatch`, `OutOfBounds` |
| `ArraySpec` | Logical and chunk shapes form a regular grid; names match rank. | `InvalidGrid`, `RankMismatch` |
| `DenseArray.copyOf` | Element count matches shape and fits dense allocation limits. | `InvalidShape`, `ResourceLimit` |
| `openTypedArray` | Stored metadata compiles and the dtype matches the requested witness. | `InvalidMetadata`, `UnsupportedExtension`, `DTypeMismatch` |
| `read…` | The selection belongs to the array and the operation stays within read/decode limits. | `InvalidSelection`, `StoreFailure`, `CodecFailure`, `ResourceLimit` |
| `create…` | Descriptor, codec runtime, provider, and writer limits are lawful. | `UnsupportedWrite`, `InvalidCodecRuntime`, or an incomplete `WriteOutcome` |

`require` still appears in limit case-class constructors for programmer errors
such as a negative `maxObjects`. Construct configuration from untrusted input
only after validating that input in the application.

## Copies mark ownership changes

`DenseArray.copyOf` copies the input array. Later changes to the caller's array
cannot change the value that `zarr4s` writes. `DenseArray.toArray` also returns
a defensive copy.

```scala mdoc:silent
val shape = checked(Shape(3L))
val source = Array[Short](10, 20, 30)
val owned = checked(DenseArray.copyOf(DType.Int16, shape, source))

source(0) = 99
val exposed = owned.toArray
exposed(1) = 88
```

```scala mdoc
owned.toArray.toVector
```

`DenseArray.adopt` removes the first copy when the caller already owns a fresh
array. After adoption, the caller must never mutate that array again. Use it at
a carefully audited adapter boundary, not as a routine performance switch.

Read results also own their primitive storage. A caller can retain a result
without depending on an internal decoder buffer or cache entry.

## Write failures retain progress

Generic object stores cannot roll back a namespace transaction. A write can
therefore return `WriteOutcome.Incomplete(progress, error)` after creating some
objects. The progress records what exists. A complete outcome includes a
`WriteReceipt` and means the final metadata completion marker was created.

This is why write outcomes should not be collapsed to a Boolean. The
[writing guide](../guides/writing-arrays.md) shows the complete handling pattern.

Next: [open an existing array](../guides/opening-arrays.md).
