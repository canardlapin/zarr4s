# How should I tune remote reads and caches?

Treat remote performance as a request-and-byte budget before treating it as a
timing problem. Chunking and sharding determine the physical work. A cache can
avoid repeating that work only when the caller can name an immutable revision.

## Cache by revision, not by URL

Wrap the reader in `CachingObjectReader` or `AsyncCachingObjectReader` and use a
fresh `CacheNamespace` for each immutable dataset revision. Reusing a namespace
after the underlying objects change can return stale data. zarr4s deliberately
does not guess revision identity or install a global cache.

The evidence court reads the same volume twice through an 8 MiB,
256-entry cache. The warm read preserves its logical receipt—the chunks or
shards still define the operation—but performs no downstream store work.

<!-- evidence:cache:start -->
| Layout | Phase | Downstream requests | Fetched bytes | Cache hits |
| --- | --- | --- | --- | --- |
| direct-balanced | cold | 8 | 262144 | 0 |
| sharded-balanced | cold | 10 | 262404 | 0 |
| direct-balanced | warm | 0 | 0 | 8 |
| sharded-balanced | warm | 0 | 0 | 10 |
<!-- evidence:cache:end -->

Size the cache from the working set, not the full array. Begin with enough
entries and bytes for one repeated interactive operation, inspect `CacheStats`
for hits, evictions, resident bytes, and downstream traffic, then expand only
when reuse justifies the memory. Exact ranges, object lengths, and whole objects
are distinct cache entries; a shard-heavy workload may therefore need more
entries than its shard count suggests.

## Match policy to the store

| Store condition | Policy to test | Evidence to inspect |
| --- | --- | --- |
| High per-request latency | Fewer touched objects; bounded parallel reads | Object/range count and elapsed time. |
| Charged requests | Larger chunks or shards when amplification remains acceptable | Downstream request count and provider bill. |
| Limited bandwidth | Smaller relevant chunks, compression, exact shard ranges | Fetched bytes and amplification. |
| Repeated immutable reads | Revision-scoped cache sized to the working set | Hits, evictions, fetched bytes. |
| Mutable or weakly versioned data | New namespace per known revision, or no cache | Revision contract and freshness tests. |

`ReadLimits.maxConcurrentRequests` bounds portable asynchronous scheduling; it
does not create a retry policy. `JvmHttpStore` and browser `FetchStore` validate
range semantics, but credential, retry, rate-limit, and revision policy remain
with the caller or a downstream store adapter.

The executable court proves exact work for an in-memory, uncompressed fixture.
It does not claim network throughput or a universal optimum. Before production,
repeat representative reads against the intended endpoint and record elapsed
time alongside the [versioned request-and-byte receipt][receipt].

Next: [set explicit cache and execution limits](cache-and-limits.md).

[receipt]: https://raw.githubusercontent.com/canardlapin/zarr4s/main/site-docs/advanced/performance-evidence.csv
