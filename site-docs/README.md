# zarr4s guide

`zarr4s` reads and creates Zarr arrays and groups from Scala 3 on the JVM and
Scala.js. It is useful when array data must remain portable across runtimes
without hiding its shape, data type, storage behavior, or I/O failures.

> **Status:** `zarr4s` is a 0.1 pre-release. The source, guide, and
> cross-platform tests are public, but no stable Maven artifact has been
> published. [Installation status](start/installation.md) explains what can be
> used today without inventing a release coordinate.

The ordinary API has three durable ideas:

1. A Zarr array has a logical shape and data type, but storage divides it into
   chunks. Reads return logical values, not encoded chunks.
2. Dynamic metadata becomes a typed handle only after its dtype has been
   checked. A requested `Int16` array cannot silently materialize as `Float32`.
3. Storage, codecs, caching, concurrency, and error policy remain explicit.
   The library does not install process-wide policy on behalf of an
   application.

## Follow one array through the guide

The learning path uses one 4 × 6 `int16` measurement array. It begins in an
in-memory store, then moves through regions, point and factored selections,
groups, filesystem and HTTP stores, codecs, sharding, and remote-read controls.
The objects keep the same names so each page adds one idea rather than starting
over.

Start with:

- [Installation status](start/installation.md) if you are adding `zarr4s` to a
  build;
- [Create and read your first array](start/first-array.md) for the shortest
  successful program; or
- [Open an existing array](guides/opening-arrays.md) if you already have a Zarr
  store.

## What the guide covers

- **Learn** explains arrays, chunks, typed values, validation, and ownership.
- **Use** follows complete tasks: opening, selecting, writing, and navigating
  groups.
- **Advanced** introduces platform boundaries, codecs, sharding, caches, and
  limits only after the basic workflow is familiar.
- **Reference** states the supported Zarr surface and maps tasks to public API.
- **Help** begins with observable failures and concrete remedies.

The low-level descriptor, codec-program, fragment, and provider APIs are not
deprecated. They serve streaming inputs, custom stores and codecs, and
fragment-level processing. The guide introduces them when a task requires that
control.
