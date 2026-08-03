# When should I use shards?

Use indexed sharding when many inner chunks should travel and be managed as one
stored object. Sharding can reduce whole-object reads for a subset because the
reader fetches the shard index and exact byte ranges. It also adds index work
and makes an outer shard the unit of publication and replacement.

The evidence below compares the `16 × 16 × 8 × 8` direct chunks from the chunk
court with shards of `32 × 32 × 16 × 16` containing inner chunks of
`16 × 16 × 8 × 8`. Values, selections, codecs, and scalar type are identical.

<!-- evidence:shards:start -->
| Layout | Workload | Object requests | Range requests | Index bytes | Total bytes | Amplification |
| --- | --- | --- | --- | --- | --- | --- |
| direct-balanced | volume | 8 | 0 | 0 | 262144 | 8.000000 |
| direct-balanced | movie-16 | 16 | 0 | 0 | 524288 | 1.000000 |
| direct-balanced | aligned-roi | 1 | 0 | 0 | 32768 | 1.000000 |
| direct-balanced | voxel-series | 8 | 0 | 0 | 262144 | 2048.000000 |
| sharded-balanced | volume | 10 | 9 | 260 | 262404 | 8.007935 |
| sharded-balanced | movie-16 | 3 | 2 | 260 | 524548 | 1.000496 |
| sharded-balanced | aligned-roi | 3 | 2 | 260 | 33028 | 1.007935 |
| sharded-balanced | voxel-series | 12 | 8 | 1040 | 263184 | 2056.125000 |
<!-- evidence:shards:end -->

For the 16-volume movie, sharding reduces requests from 16 to 3 while adding
260 index bytes. It does not help every operation: the one-volume and
voxel-series reads issue more requests because each touched shard needs length,
index, and data-range operations. A store may price or schedule those operations
differently, so byte amplification alone cannot decide.

Choose an outer shard only after choosing useful inner chunks. Then keep the
outer shard large enough to reduce object count, but small enough that
publication, replacement, index reads, and cache residency remain acceptable.
Measure the real store: range latency, request pricing, and whether an
intermediary honors ranges can reverse a local result.

The table is checked against the same [versioned executable receipt][receipt]
as the chunk guide.

Next: [choose cache and remote-store policy](remote-performance.md).

[receipt]: https://raw.githubusercontent.com/canardlapin/zarr4s/main/site-docs/advanced/performance-evidence.csv
