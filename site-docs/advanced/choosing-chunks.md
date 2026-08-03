# How should I choose chunk shapes?

Choose chunks from the reads that must be efficient. A chunk is the smallest
independently stored unit in a direct array, so every touched chunk contributes
its complete encoded bytes even when a selection needs only one value.

Start by writing down representative selections. Make frequently read regions
touch few chunks, but keep chunks large enough that object-request overhead does
not dominate. There is no shape-independent default.

## What the repository evidence measures

The executable court stores an uncompressed `Int16` array with shape
`32 × 32 × 16 × 64`, ordered as `x, y, z, time`. It compares:

- `direct-balanced`: chunks of `16 × 16 × 8 × 8`;
- `direct-volume`: chunks of `32 × 32 × 16 × 1`.

The workloads read one spatial volume, 16 adjacent volumes, one chunk-aligned
region, and one voxel through all 64 time points. Counts come from
`ExecutionReceipt`; cache traffic comes from `CacheStats`. The test suite
recreates the arrays, checks equal values across layouts, and requires this
table and the [complete CSV receipt][receipt] to match the executable result.

<!-- evidence:chunks:start -->
| Layout | Workload | Object requests | Bytes read | Amplification |
| --- | --- | --- | --- | --- |
| direct-balanced | volume | 8 | 262144 | 8.000000 |
| direct-balanced | movie-16 | 16 | 524288 | 1.000000 |
| direct-balanced | aligned-roi | 1 | 32768 | 1.000000 |
| direct-balanced | voxel-series | 8 | 262144 | 2048.000000 |
| direct-volume | volume | 1 | 32768 | 1.000000 |
| direct-volume | movie-16 | 16 | 524288 | 1.000000 |
| direct-volume | aligned-roi | 8 | 262144 | 8.000000 |
| direct-volume | voxel-series | 64 | 2097152 | 16384.000000 |
<!-- evidence:chunks:end -->

The volume-oriented layout is the clear choice when full spatial volumes are
the main operation: one request and no excess bytes. The balanced layout is
better for the aligned region and the voxel series. Both layouts read exactly
the requested bytes for the 16-volume movie, but the result does not include
network latency; 16 objects can still cost more wall-clock time than fewer,
larger objects on a remote store.

Use the same method on your data:

1. Name the important selections and their expected frequency.
2. Create two or three plausible layouts in a disposable store.
3. Read each selection and compare request count, bytes, and amplification.
4. Reject layouts that make a critical workflow expensive; then measure elapsed
   time against the intended store.

Compression changes byte counts, not the selection geometry. Re-run the court
with the real codec chain before making a production choice.

Next: [decide whether to add shards](choosing-shards.md).

[receipt]: https://raw.githubusercontent.com/canardlapin/zarr4s/main/site-docs/advanced/performance-evidence.csv
