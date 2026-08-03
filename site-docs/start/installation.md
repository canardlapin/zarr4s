# Installation status

No stable `zarr4s` artifact has been published. Do not add a guessed version to
`libraryDependencies`: a coordinate that looks plausible but cannot be
resolved is worse than an explicit pre-release workflow.

## Use a local publication

Clone the repository and publish the platform projections that your build
needs:

```text
git clone https://github.com/canardlapin/zarr4s.git
cd zarr4s
sbt 'coreJVM/publishLocal' 'coreJS/publishLocal'
```

Ask sbt for the exact snapshot version produced by the checkout:

```text
sbt 'show coreJVM/version'
```

Use that value in a JVM-only Scala 3 build:

```scala
libraryDependencies +=
  "io.github.canardlapin" %% "zarr4s-core" % "<version-from-sbt>"
```

In a Scala.js or JVM/Scala.js cross-project, use the platform-aware operator:

```scala
libraryDependencies +=
  "io.github.canardlapin" %%% "zarr4s-core" % "<version-from-sbt>"
```

The repository includes a
[standalone consumer](https://github.com/canardlapin/zarr4s/tree/main/examples/standalone-consumer)
that performs this local-publication workflow and compiles the public API on
both platforms.

## Choose the module

| Module | Add it when | Additional runtime boundary |
| --- | --- | --- |
| `zarr4s-core` | You need metadata, arrays, groups, stores, reads, create-only writes, or built-in codecs. | None in the shared core. JVM and browser transports use their platform libraries. |
| `zarr4s-codec-blosc-zstd` | Existing data requires Blosc or standalone Zstandard, or you choose those codecs when writing. | `blosc-java` and `zstd-jni` on the JVM; pinned npm packages on Scala.js. |

For the optional Scala.js codec provider, install its pinned JavaScript
dependencies before compiling it:

```text
npm ci --prefix codec-blosc-zstd/js
```

## What changes after the first release

Only the acquisition step should change. The package imports and the typed
workflow in this guide use the intended public API. A release should replace
this page's local snapshot instructions with a real version and should publish
matching API documentation.

Next: [create and read your first array](first-array.md).
