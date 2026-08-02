# Standalone `zarr4s-core` consumer gate

This deliberately separate sbt build has exactly one library dependency:
the locally published `zarr4s-core` artifact. Its shared consumer defines an
external codec schema and both execution capabilities, then compiles the public
metadata, runtime, immutable object-store, portable `AsyncZarr` reader,
revision-scoped sync/async cache, sync/async create-only writer APIs, and the
typed README quickstart on the JVM and Scala.js. `StandaloneConsumerMain` runs
that typed quickstart and checks the returned values.

From the repository root:

```sh
sbt 'coreJVM/publishLocal' 'coreJS/publishLocal'
cd examples/standalone-consumer
sbt 'consumerJVM/compile' 'consumerJS/compile'
sbt 'consumerJVM/runMain example.StandaloneConsumerMain' 'consumerJS/run'
```

The consumer derives the current git snapshot version used by sbt-typelevel.
Pass `-Dzarr4s.version=<version>` to compile against another local or released
version.
