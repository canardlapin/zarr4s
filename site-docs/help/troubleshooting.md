# Troubleshooting

Start with the returned error value and the operation that produced it. Avoid
retrying every `ZarrError`: malformed metadata, dtype mismatches, unsupported
extensions, and resource limits do not become valid through repetition.

## The dependency cannot be resolved

**Symptom:** sbt reports that `io.github.canardlapin` / `zarr4s-core` cannot be
found.

**Cause:** no stable artifact has been published, or the local snapshot version
does not match the checkout that was published locally.

**Remedy:** run `coreJVM/publishLocal` and/or `coreJS/publishLocal`, ask sbt for
`coreJVM/version`, and use that exact value. See
[Installation status](../start/installation.md).

## Opening reports `DTypeMismatch`

**Symptom:** `openTypedArray(store, DType.Float32)` reports that the stored
array is another dtype.

**Cause:** the requested witness does not match compiled metadata.

**Remedy:** correct the expected dtype. If the application accepts several
dtypes, call `openArray`, inspect `descriptor.dataType`, and refine through the
matching typed branch. Do not cast the returned primitive block.

## Opening reports a missing or invalid codec runtime

**Symptom:** metadata parses, but opening fails with `InvalidCodecRuntime`,
`UnsupportedExtension`, or a missing executor message.

**Cause:** codec metadata support and codec execution are separate. For
example, `SyncCodecRuntime.core` does not execute JVM gzip, and the core
artifact does not contain Blosc or standalone Zstandard.

**Remedy:** pass `JvmCodecRuntime.portable` for JVM gzip/zlib, the browser
runtime on Scala.js, or both the optional provider capabilities and its
platform runtime for Blosc/Zstandard.

## An HTTP read reports `RangeIgnored`

**Symptom:** a remote partial read fails even though a plain browser download
of the object works.

**Cause:** the server returned a full `200` response instead of honoring the
requested byte range with a valid `206` and `Content-Range`.

**Remedy:** configure the object host or proxy to support byte ranges. The
store rejects an ignored range rather than treating the full body as the
requested slice.

## `group.children` says discovery is unsupported

**Symptom:** a group opens, but `children` returns `UnsupportedRead`.

**Cause:** the metadata has no supported consolidated index and no listing
capability was supplied.

**Remedy:** open known children by path, provide an `ObjectLister`, or publish
supported consolidated metadata. `JvmFileStore` and `MemoryStore` implement
listing; `JvmHttpStore` and `FetchStore` do not.

## A second write is incomplete with `AlreadyExists`

**Symptom:** recreating an array at the same path returns
`WriteOutcome.Incomplete`.

**Cause:** writing is create-only.

**Remedy:** choose a new path or revision outside `zarr4s`. Inspect
`WriteProgress` before cleanup; a generic store can contain objects from an
earlier incomplete publication. Do not implement overwrite by catching the
error and deleting an unverified prefix.

## An operation exceeds a resource limit

**Symptom:** the result is `ZarrError.ResourceLimit`.

**Cause:** metadata, decoded chunks, requests, selection planning, cache
residency, or write volume exceeded an explicit bound.

**Remedy:** inspect the named resource, requested amount, array metadata, and
selection. Raise the specific limit only after deciding that the input is
trusted and the allocation or I/O is acceptable.

## Scala.js optional codecs fail to link or run

**Symptom:** imports from the optional provider compile incompletely, npm
modules are missing, or browser codec initialization fails.

**Cause:** the pinned JavaScript dependencies were not installed for
`codec-blosc-zstd/js`, or the provider runtime was not supplied.

**Remedy:** run `npm ci --prefix codec-blosc-zstd/js`, rebuild the Scala.js
projection, and use `BrowserBloscZstdRuntime.portable` with provider
capabilities.

## A filesystem store will not open

**Symptom:** `JvmFileStore.open` returns a string error.

**Cause:** the root does not exist, is not a directory, or cannot be resolved
to a safe real path.

**Remedy:** create or locate the Zarr directory first. Use `JvmZarr.create…`
when publishing a new target; use `JvmFileStore.open` for an existing store.

If the error remains unclear, reduce the case to one metadata object and one
selection, record the exact `ZarrError`, format version, dtype, codec chain,
store type, and relevant limits, and include those facts in a bug report.
