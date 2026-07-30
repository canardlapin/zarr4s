# Zarr Z5 layout and codec evidence

This benchmark decides what NeuroArchive Zarr 0.1 promotes; it is not a claim
that one layout is optimal for every neuroimaging operation. The reproducible
driver remains in the
[`canardlapin/scalafim`](https://github.com/canardlapin/scalafim/blob/main/tools/benchmark_zarr_profiles.py)
repository because it measures a NeuroArchive workload rather than the generic
library alone.

## Method

The request model uses a representative canonical BOLD array of shape
`[1200,72,96,96]` in `[t,z,y,x]` order with two-byte stored scalars. It measures
nominal inner-chunk bytes plus the start-indexed shard prefix for four access
patterns: one volume, a 32-volume movie, a compact 32-volume ROI, and a
single-voxel time-series proxy. The proxy intentionally exposes the point at
which a canonical volume should yield to a future masked analytical layout.

Codec measurements used six deterministic, spatially structured and
temporally correlated int16 chunks per profile, with three repetitions. The
run was made on 2026-07-20 on Apple arm64/macOS 14.3 with Python 3.9.22,
NumPy 1.26.4, and numcodecs 0.12.1. Throughput is machine- and corpus-specific;
the request geometry is deterministic.

## Layout result

| Profile | Inner chunk `[t,z,y,x]` | Shard `[t,z,y,x]` | Chunk MiB | Shard MiB | 1 volume amplification | 32-volume amplification | ROI amplification | Voxel-series amplification | Weighted requests |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| canonical-balanced-0.1 | 16,24,32,32 | 64,72,96,96 | 0.75 | 81 | 16.00 | 1.00 | 1.00 | 24589.71 | 32.6 |
| volume-oriented | 4,24,32,32 | 32,72,96,96 | 0.19 | 40.5 | 4.00 | 1.00 | 1.00 | 24630.78 | 100.2 |
| temporal-oriented | 64,8,16,16 | 256,32,64,64 | 0.25 | 64 | 64.04 | 2.00 | 2.01 | 2083.85 | 192.1 |

`canonical-balanced-0.1` is the 0.1 default. Its 768 KiB int16 chunks avoid the
small-object/request explosion of the volume profile, its 81 MiB shards are in
the intended object-store range, and it is efficient for movie and compact ROI
reads. Float32 uses the same geometry with 1.5 MiB chunks and 162 MiB shards.

The single-voxel result is a boundary finding, not a defect to hide: no dense
canonical layout is a good time-by-voxel store. A later masked projection should
serve that workload. Changing the canonical chunks to imitate the missing
projection would make volumes and ROI windows worse while multiplying requests.

## Codec result

For the promoted balanced geometry:

| Codec | Compression ratio | Encode MiB/s | Decode MiB/s |
| --- | ---: | ---: | ---: |
| gzip level 1 | 1.304 | 63.6 | 336.5 |
| Blosc + Zstandard level 3 + shuffle | 1.569 | 1071.1 | 3609.3 |
| Blosc + Zstandard level 3 + bitshuffle | 1.525 | 1792.5 | 5389.0 |

Blosc/Zstandard clearly earns continued work, but it does not pass the 0.1
capability gate. Promoting it now would add a native/JVM dependency without a
comparably small and dependable Scala.js decoder. NeuroArchive 0.1 therefore
keeps the portable, chunk-local `bytes(little) -> gzip(1) -> crc32c` chain.
The generic codec compiler remains extensible, so a separately packaged fast
capability can be added after JVM, Scala.js, Python, corruption, and deployment
tests all pass.

## Production verdicts

- Promote the measured canonical chunk/shard geometry as a profile value, not
  a generic Zarr default.
- Keep start-indexed shards mandatory for NeuroArchive; the generic reader and
  writer continue to execute standard end-indexed shards when object length is
  available.
- Keep gzip as the 0.1 portable codec and do not add a heavy codec dependency.
- Do not add credentialed S3 yet. Existing HTTP range readers cover public and
  presigned object URLs; authentication policy belongs in a later adapter when
  an actual deployment requires it.
- Expose request counts, length requests, index bytes, data bytes, logical
  bytes, and read amplification in execution receipts. Timing and cache status
  remain runtime concerns rather than hidden global effects in the kernel.
