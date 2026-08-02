# Zarr Z9 common support matrix and conformance court

This document is the working boundary for the `Complete common core Zarr
support` epic. It records what zarr4s can claim, what the next slices must
prove, and which features remain deliberately outside the generic kernel.

The initial matrix baseline was commit `4ff4532` on 2026-08-01. The matrix is
intentionally implementation-specific: a feature is not admitted because a
metadata parser recognises its name. It is admitted only after the relevant
lowering, cross-platform execution, bounded failure, and interoperability
evidence is present.

## Design authority

The repository keeps one semantic descriptor and one codec-program model for
both Zarr formats. V2 metadata lowers into that model; it does not introduce a
second reader, writer, or scalar ownership hierarchy. Optional algorithms stay
in provider artifacts. The `zarr4s-core` artifact remains dependency-free and
cross-compiles to the JVM and Scala.js.

The external normative references are:

- [Zarr v3 core](https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html)
- [Zarr v3 data types](https://zarr-specs.readthedocs.io/en/stable/v3/data-types/index.html)
- [Zarr v3 codecs](https://zarr-specs.readthedocs.io/en/latest/v3/codecs/index.html)
- [Zarr v3 sharding](https://zarr-specs.readthedocs.io/en/latest/v3/codecs/sharding-indexed/index.html)
- [Zarr v3 filesystem store](https://zarr-specs.readthedocs.io/en/latest/v3/stores/filesystem/index.html)
- [Zarr v2 storage specification](https://zarr-specs.readthedocs.io/en/latest/v2/v2.0.html)

These references define the external contract. They do not by themselves
constitute evidence that this implementation supports a feature.

## Support matrix

### Current claims

| Surface | Current support | Evidence boundary |
| --- | --- | --- |
| V3 hierarchy | Groups, arrays, explicit navigation, bounded inline consolidation | JVM and Scala.js hierarchy suites and independent metadata fixtures |
| V3 grid and keys | Regular grids, default keys, v2-compatible keys, scalar and arbitrary runtime rank | Shared geometry, grid, key, reader, and writer suites |
| V3 data types | `bool`, signed and unsigned integers 8/16/32/64, `float16`, `float32`, `float64`, `complex64`, `complex128`, and byte-multiple raw `rN` | Exact fill, endian, byte, selection, transpose, and independent Zarr-Python payload tests |
| V3 common codecs | Bytes, transpose, gzip, CRC32C, and indexed sharding at start or end | Direct and sharded JVM/Scala.js fixtures, including writer output |
| Optional codecs | Blosc and standalone Zstandard in `zarr4s-codec-blosc-zstd` | Provider metadata, bounded/corruption, direct, sharded, JVM, and Scala.js suites |
| V2 reading | Groups and arrays lowered into the shared descriptor; C/F order; endian; dot/slash keys; consolidated metadata | V2 metadata, hierarchy, reader, and external compatibility fixtures |
| V2 codecs | gzip, zlib, common shuffle, and optional Blosc/Zstandard provider paths | V2 metadata and end-to-end fixtures; unsupported codecs fail typed |
| Writing | Create-only V3 arrays and groups over sync/async object capabilities | Shared writer and JVM filesystem publication suites |
| Stores | Whole-object, range, and length reads; immutable object creation; bounded memory, filesystem, HTTP, and Fetch adapters | Store and transport suites |

### Planned common slices

| Slice | Intended claim | Required proof |
| --- | --- | --- |
| Listing capability | Optional `ObjectLister` and `AsyncObjectLister`; un-consolidated child discovery where the store can list | Memory/JVM listing, bounded discovery, explicit HTTP/Fetch capability behavior |
| V2 delta | Dtype-aware v2 delta filter with correct `dtype`/`astype` and reverse execution | Independent delta fixtures, C/F and endian combinations, shuffle/compressor composition |
| Sharding parity | Outer codec support and lawful fixed-size index codec profiles | Bounded whole-shard fallback, range fast path, writer output, and corruption fixtures |
| V2 creation | Create-only `.zarray`, `.zattrs`, and `.zgroup` output from the shared descriptor | Zarr-Python and zarrs readback, deterministic keys, fill omission, interruption receipts |

## Explicit non-claims

The following remain unsupported until a concrete, lawful contract is added:

- V2 creation, overwrite, resize, append, deletion, and concurrent mutation.
- V2 object, variable-length, structured, string, datetime, and timedelta data.
- V2 filters other than shuffle until their typed array semantics are implemented.
- V3 variable-length and object-like data types.
- Raw `rN` values are limited to positive, byte-multiple widths and do not
  claim object, string, or structured semantics.
- V3 storage transformers and non-regular chunk grids without a required
  standard extension and a bounded partial-read model.
- Credentials, retries, persistent caches, prefetch, retention, and scheduler
  policy in the generic kernel.

Unsupported required metadata must continue to fail through `ZarrError`.
Optional unknown extensions may remain ignorable only where the metadata
contract permits that behavior. A name match, a successful compile, or a
provider-only round trip is not enough to expand these claims.

## Conformance court

Every feature slice must pass the courts that apply to its scope.

### 1. Contract court

- Parse valid and malformed metadata with precise paths and typed errors.
- Validate defaults, required fields, shape/rank laws, fill values, and codec
  representation transitions.
- Preserve unknown optional metadata without silently accepting unknown required
  behavior.

### 2. Semantic court

- Prove encode/decode inversion on empty, scalar, border, and arbitrary-rank
  chunks.
- Prove C/F order, endian behavior, duplicate/unsorted selections, fill chunks,
  and sharded padding where relevant.
- Check exact byte lengths before allocation and enforce all configured limits.

### 3. Platform court

- Run the shared suite on both JVM and Scala.js.
- Use the same fixture bytes and expected values on both platforms.
- Keep blocking JVM work behind caller-supplied execution contexts and keep
  optional JNI/WASM/JavaScript code outside core.

### 4. External court

- Add at least one independent Zarr-Python or zarrs fixture for every new
  encoded format.
- Record producer, version, source commit or checksum, and attribution beside
  the fixture.
- Where writing is in scope, have an independent reader reconstruct the output;
  a self-read is not sufficient.

### 5. Adversarial court

Cover truncated and corrupted payloads, forged decoded lengths, invalid
configuration, unsupported identifiers, over-limit metadata/chunks/shards,
missing objects, ignored ranges, and partial indexes. The expected result must
be a typed error or lawful fill behavior, never an unbounded allocation or a
guessed interpretation.

### 6. Release court

The repository gate for every affected slice is:

```text
npm ci --prefix codec-blosc-zstd/js
sbt checkAll
```

The slice must also update the support boundary and preserve the independent
standalone consumer. A green unit suite without cross-platform compilation,
external fixture evidence, or a clean dependency boundary is incomplete.

## Order of work

1. Keep this matrix and the independent-fixture rules current.
2. Add listing as an optional store capability so consolidated metadata is not
   the only route to hierarchy discovery.
3. Generalize array-codec execution and implement v2 delta.
4. Complete sharding outer/index parity, then add create-only v2 writing.
5. Admit additional optional codecs or extensions only after their own provider
   and deployment courts pass.

The epic is complete only when each planned claim has its required evidence or
has been explicitly closed as a deferred non-claim with a reason.
