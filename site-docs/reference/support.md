# Supported formats and data

This page states the 0.1 support boundary. Recognition of a metadata name is
not enough: a feature is listed as supported only when the repository contains
the relevant metadata, execution, bounded-failure, and platform tests. Encoded
formats also have independent fixture or readback evidence where applicable.

## Platforms and effects

| Surface | JVM | Scala.js |
| --- | --- | --- |
| Shared metadata, geometry, planning, typed arrays, selections, sync/async capability interfaces | Yes | Yes |
| Portable synchronous interpreter | Yes | Compiles, but browser I/O is asynchronous; use `AsyncZarr` or `BrowserZarr`. |
| Portable asynchronous interpreter | Yes | Yes |
| Filesystem store and staged `Path` publication | Yes | No |
| Checked HTTP range store | Yes | No |
| Fetch range store | No | Yes |
| In-memory sync and async stores | Yes | Yes |

The executable site examples run against `coreJVM`. The repository's JVM and
Scala.js suites, not the site build alone, establish cross-platform coverage.

## Optional Ravel arrays

`zarr4s-interop-ravel` provides first-party optional support for immutable
Ravel NDArrays on the JVM and Scala.js. It maps `bool`, signed 8/16/32/64-bit
integers, unsigned 8/16-bit integers, and 32/64-bit floating values exactly.
Other zarr4s dtypes remain unsupported at this boundary rather than being
widened. Zarr dimensions and total element counts must fit Ravel's `Int`
shape model.

The adapter is tested but not published because Ravel has no immutable release.
See [use zarr4s with Ravel arrays](../guides/ravel-interop.md) for the ownership,
selection, write, and local-consumer contract.

## Zarr formats

| Area | Supported in 0.1 |
| --- | --- |
| Zarr v3 nodes | Arrays and groups; regular grids; default and v2-compatible chunk keys; explicit navigation; bounded inline consolidation; listing-backed discovery. |
| Zarr v2 nodes | Arrays and groups lowered to the shared descriptor; C and Fortran order; dot and slash chunk keys; consolidated and listing-backed unconsolidated metadata. |
| Rank | Runtime rank, including rank-zero scalar arrays and zero-length dimensions. Default open limit is 32 axes. |
| Selection | Full reads, bounded rectangular regions, ordered point batches, and factored `All`, positive-step `Slice`, and ordered `Indices`. |
| Writing | Create-only v3 and common v2 arrays and groups over synchronous or asynchronous object creation. |
| Sharding | Zarr v3 `sharding_indexed` reads and writes with start or end indexes and the supported index-codec profile. |

## Typed dense data

| Zarr dtype | Scala carrier in `DenseArray` |
| --- | --- |
| `bool` | `Boolean` |
| `int8`, `uint8` | `Byte` |
| `int16`, `uint16` | `Short` |
| `int32`, `uint32` | `Int` |
| `int64`, `uint64` | `Long` |
| `float16`, `float32` | `Float` |
| `float64` | `Double` |
| `complex64` | `Complex64Value` |
| `complex128` | `Complex128Value` |

Unsigned carriers preserve the bit pattern and remain distinct through their
`DType` witness. `float16` converts between Scala `Float` and binary16 at the
typed boundary.

The dynamic descriptor layer also supports positive byte-multiple raw `rN`
carriers. They are not part of the typed `DenseArray` façade and do not imply
string, object, or structured semantics.

## Codecs

Core supports:

- bytes with little- or big-endian encoding;
- transpose;
- gzip and Zarr v2 zlib;
- CRC32C;
- common Zarr v2 shuffle; and
- dtype-aware Zarr v2 delta for fixed-width boolean, integer, and floating
  arrays.

The optional `zarr4s-codec-blosc-zstd` artifact supports Blosc with Zstandard
and standalone Zstandard on the JVM and Scala.js. Applications must supply its
metadata capability and matching platform runtime.

## Explicit non-support

The 0.1 line does not claim:

- overwrite, append, resize, deletion, or concurrent mutation;
- variable-length, string, object, structured, datetime, or timedelta values;
- v3 storage transformers or non-regular chunk grids;
- descending slices;
- every v2 filter or Zarr extension;
- S3 or other cloud SDKs, credentials, retries, persistent caches, prefetch,
  retention, or scheduler policy; or
- transactional rollback for a generic object store after an incomplete
  write.

Unsupported required metadata returns `ZarrError`. The reader does not guess a
nearby interpretation.

For the evidence and historical environment behind these claims, see the
repository's
[common support matrix](https://github.com/canardlapin/zarr4s/blob/main/docs/plans/zarr-z9-common-zarr-support.md).

Next: [map a task to the public API](api-map.md).
