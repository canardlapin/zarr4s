# Standalone `scalafim-zarr` consumer gate

This deliberately separate sbt build has exactly one library dependency:
the locally published `scalafim-zarr` artifact. Its shared consumer defines an
external codec schema and both execution capabilities, then compiles the public
metadata, runtime, immutable object-store, portable `AsyncZarr` reader,
revision-scoped sync/async cache, and sync/async create-only writer APIs on the
JVM and Scala.js.

From the repository root:

```sh
sbt 'zarrJVM/publishLocal' 'zarrJS/publishLocal'
cd tools/zarr-standalone-consumer
sbt 'consumerJVM/compile' 'consumerJS/compile'
```
