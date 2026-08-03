# Arrays, chunks, and typed values

Zarr separates the array a program uses from the objects a store holds. The
logical array has a shape and dtype. A regular grid divides it into chunks for
storage. Codecs then transform each stored chunk into bytes.

For the guide's 4 × 6 array with 2 × 3 chunks:

```text
logical values                 stored chunk grid

 1  2  3 |  4  5  6             (0,0)  (0,1)
 7  8  9 | 10 11 12
---------+---------               (1,0)  (1,1)
13 14 15 | 16 17 18
19 20 21 | 22 23 24
```

Users select logical coordinates. The reader determines which chunks contain
them, fetches and decodes those chunks, and assembles the requested output.
Chunk coordinates and encoded bytes remain storage details unless an advanced
workflow asks for them.

## The four values in the ordinary workflow

| Value | Question it answers |
| --- | --- |
| `ArraySpec[D]` | What array should be created: dtype, logical shape, chunk shape, format, fill value, names, and attributes? |
| `DenseArray[D]` | Which owned primitive values should be written? |
| `TypedOpenedArray[D]` | Has dynamic metadata been checked against the dtype the program expects? |
| `TypedReadResult[D]` | Which owned values were read, and what physical I/O produced them? |

The type parameter `D` names a `DType` witness such as `DType.Int16.type`.
Rank remains a runtime value because scientific arrays commonly acquire their
shape from files or user input. The split is deliberate: Scala prevents an
`Int16` dense value from being passed where a `Float32` dense value is
required, while `Shape` validates dynamic dimensions and arithmetic.

```scala mdoc:compile-only
import zarr4s.*

val shape: Either[ZarrError, Shape] = Shape(4L, 6L)
val chunks: Either[ZarrError, Shape] = Shape(2L, 3L)

val specification: Either[ZarrError, ArraySpec[DType.Int16.type]] =
  for
    foundShape <- shape
    foundChunks <- chunks
    found <- ArraySpec(DType.Int16, foundShape, foundChunks)
  yield found
```

## Dynamic metadata becomes typed once

An existing Zarr store does not carry Scala types. `openArray` therefore
returns a dynamic `OpenedArray` whose descriptor names a runtime data type.
`openTypedArray(store, DType.Int16)` or `opened.asTyped(DType.Int16)` checks
that descriptor before returning a typed handle. A mismatch is
`ZarrError.DTypeMismatch`; values are never reinterpreted to satisfy the
requested type.

This distinction matters most for unsigned types. `DType.UInt16` uses
`Array[Short]` as its compact carrier, but the witness remains distinct from
`DType.Int16`. The bit pattern is preserved; callers interpret the carrier as
unsigned according to the dtype.

## What the model does not decide

The array model does not choose credentials, retry rules, a global cache,
thread pools, chunk sizes, compression, or retention. Those choices depend on
the application and store. `zarr4s` represents them as arguments or explicit
capabilities so two callers in the same process can use different policies.

Next: [see where validation and ownership change](errors-and-ownership.md).
