# Z8 portable remote-cache receipt

Date: 2026-07-22

Executable gate: `FmriRemoteCacheSuite` plus `ObjectReadCacheSuite` on JVM and
Scala.js

Status: passed in the focused Z8 run and the final repository-wide run

## Question

Can the generic Zarr core reuse an fMRI-shaped partial read without another
transport request, while keeping revision identity, memory bounds, and
scheduling explicit?

## Fixture

The deterministic rank-four array is deliberately neuroimaging-shaped but uses
only generic Zarr mechanics:

| Property | Value |
|---|---:|
| Logical shape | `[96, 96, 72, 1200]` |
| Dimension names | `x, y, z, time` |
| Scalar | `int16`, little endian |
| Chunk shape | `[32, 32, 24, 16]` |
| Requested origin | `[16, 16, 12, 400]` |
| Requested extent | `[32, 32, 24, 32]` |
| Intersected chunks | 16 |
| Encoded bytes per chunk | 786,432 (0.75 MiB) |
| Requested logical bytes | 1,572,864 (1.5 MiB) |

The first read intentionally has 8x chunk amplification: it transfers 16 whole
uncompressed chunks, or 12,582,912 bytes (12 MiB). This experiment is about
repeat remote transfer, not claiming that the chosen chunk shape is optimal for
the request.

## Receipt contract

| Measurement | First read | Identical second read |
|---|---:|---:|
| Logical object requests | 16 | 16 |
| Downstream object requests | 16 | 0 |
| Downstream bytes fetched | 12,582,912 | 0 |
| Cache hits | 0 | 16 |
| Bytes served | 12,582,912 | 12,582,912 |
| Read amplification reported by array interpreter | 8.0 | 8.0 |

The array interpreter's receipt counts logical requests to its immediate
`ObjectReader`; the cache receipt separately counts actual downstream transfer.
Keeping both is intentional: a cache hit does not rewrite the scientific read
plan, but it does eliminate transport.

## Laws exercised

- Every cache is constructed with a non-empty immutable `CacheNamespace`.
- Exact ranges, containing ranges, whole objects, and lengths are reusable.
- Entry and byte budgets use deterministic least-recently-used eviction.
- Cached bytes are copied on insertion and return.
- Concurrent identical async requests share one in-flight downstream request.
- Store errors and failed futures are removed before callers observe
  completion, so the next request retries.
- `ReadLimits.maxConcurrentRequests` bounds fetch scheduling on both JVM and
  Scala.js.
- JVM HTTP and Scala.js Fetch rereads issue no second transport range request.

The same conformance slice also reads a fixture written by
[`zarrs`](https://github.com/zarrs/zarrs) at commit
`cf8209811f5937cbe4594a7a3445b95c9d35872c`: two end-indexed shards containing
uint16 data, gzip-compressed inner chunks, and CRC32C indexes. This supplements
the existing Zarr-Python differential corpus with an independent
implementation.

The epic's explicitly named
[`zarr_implementations`](https://github.com/zarr-developers/zarr_implementations)
gate is separate. At commit
`185c4f93e114eb91dfea4e21be45a86eadca59f2`, the bounded fixture pins the
7,953-byte `examples/zarr.zr/gzip/0.0.0` chunk at SHA-256
`0cb1c9661645595aafbc4c6d97b49e3882faaa6eb73606cece730cebda4b300e`.
JVM and Scala.js open its Zarr v2 group and uint8 gzip array through ordinary
hierarchy lowering, then compare a `4 x 4 x 1` region with the corpus reference
image. The repository's checked-in v3 example follows an older draft and is
therefore not claimed as current-v3 conformance.

## Final execution receipt

The final focused run passed 139 `zarrJVM` tests and 147 `zarrJS` tests with no
failures. That includes the cache laws, single-flight and retry behavior,
bounded scheduling, the fMRI-shaped reread, JVM HTTP and Scala.js Fetch traces,
the independent `zarrs` shard, the official `zarr_implementations` fixture, and
the reciprocal zarr-java fixture. The subsequent repository-wide
`compileAll testAll` run also passed.

## Non-claims

This is a bounded in-memory read cache, not a retention system. It provides no
TTL, persistence, retry, invalidation service, credentials, S3 SDK, service
worker, or NeuroArchive policy. Callers rotate the namespace when a store
revision changes.
