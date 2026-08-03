# Ravel standalone consumer

This project compiles and runs a domain-neutral Float32 workflow against artifacts published to the
local sbt repository. It does not add either the zarr4s or Ravel checkout as an sbt source
dependency.

No public immutable Ravel or zarr4s release exists yet. The verified pre-release court publishes commit-labelled artifacts to the local Maven repository, then copies this consumer to a clean directory before resolving them.

## Publish the pinned Ravel artifact locally

From a Ravel checkout at commit `d0f7bacfe3b750519dc49aca8fd466ef70ef24ec`:

```text
sbt -batch \
  'set ThisBuild / version := "0.0.0-d0f7bac"' \
  coreJVM/publishLocal coreJS/publishLocal
```

## Publish the adapter artifacts locally

From the zarr4s checkout containing Z11:

```text
sbt -Dravel.version=0.0.0-d0f7bac -batch \
  'set ThisBuild / version := "0.1.0-z11.dcffada"' \
  'set interopRavelJVM / publish / skip := false' \
  'set interopRavelJS / publish / skip := false' \
  coreJVM/publishLocal coreJS/publishLocal \
  interopRavelJVM/publishLocal interopRavelJS/publishLocal
```

The adapter remains `publish / skip := true` in the normal build. The in-memory override above is a consumer court, not a public release.

## Compile and run

Copy this directory outside the zarr4s checkout, then run:

```text
sbt -batch consumerJVM/compile consumerJS/compile \
  consumerJVM/run consumerJS/run
```

Expected output on each platform:

```text
transformed = [2, 3, 4, 5, 6, 7]
```

The consumer creates an input Zarr array from an immutable canonical Ravel array, reads it back as an owned Ravel array, performs a Ravel `map`, creates a transformed Zarr array, and verifies the output through the adapter. It uses no `PrimitiveBlock`, owned byte carrier, `ChunkProvider`, or manual linear indexing.

## Verified receipt

On 2026-08-03, the directory was copied from candidate revision
`dcffada0188ac35c30ebe93b74cf862d1e9f04b9` to
`/private/tmp/zarr4s-ravel-consumer-exact.VUuscH/consumer`. Both platform compiles and runs
passed. `externalDependencyClasspath` resolved zarr4s and Ravel only from the
local Ivy artifact repository; it contained no project or sibling-checkout
classpath entry.

The locally published, unsigned verification jars had these SHA-256 hashes:

```text
984a9e660fc00aa6a87f6df9c0930e6bb8b0394cc5d03024f48751928d070c4f  zarr4s-interop-ravel_3.jar
f928e406cfc02dc53f30e4490b853f84c452f89523f0ece87e430a5b8a9aa2e1  zarr4s-interop-ravel_sjs1_3.jar
a198de8c9f1a097672175be49666cb444eb18fd89895a146950fc291b927058d  ravel-core_3.jar
93ae83f5028149e1cf4dc9892ef59fc5966c0c9ba098b4b4df9871fa7c5a6f7b  ravel-core_sjs1_3.jar
```

These hashes are evidence for the local court, not a public release or a
substitute for signed repository publication.
