# Standalone `zarr4s-core` consumer gate

This deliberately separate sbt build has exactly one library dependency:
the locally published `zarr4s-core` artifact. Its shared consumer defines an
external codec schema and both execution capabilities, then compiles the public
metadata, runtime, immutable object-store, portable `AsyncZarr` reader,
revision-scoped sync/async cache, and sync/async create-only writer APIs on the
JVM and Scala.js.

From the repository root:

```sh
sbt 'coreJVM/publishLocal' 'coreJS/publishLocal'
cd examples/standalone-consumer
sbt 'consumerJVM/compile' 'consumerJS/compile'
```
