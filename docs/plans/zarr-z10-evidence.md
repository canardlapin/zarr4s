# Z10 typed usability evidence

This note records the verification run for the typed usability façade. The
candidate was the uncommitted working tree at base commit
`1da4c4699de1aed09b3c27a9c9b16515524cf74f` on 2026-08-02. The working tree
also contained the pre-existing README edit and the new Z10 implementation;
no release or Git commit was created by this run.

## Toolchain

- Scala 3.7.4; root sbt 1.11.7; standalone-consumer sbt 1.10.5.
- Node v24.1.0 and npm 11.3.0.
- `npm ci --prefix codec-blosc-zstd/js` passed with an isolated temporary npm
  cache: three packages installed and zero vulnerabilities.
- Zarr-Python 3.2.1 and NumPy were resolved by `uvx`.
- The Rust oracle used zarrs 0.23.13 with Cargo 1.91.1.

## Repository gates

The following commands passed:

```text
sbt scalafmtAll coreJVM/test coreJS/test
sbt checkAll
```

The focused run passed 231 JVM tests and 235 Scala.js tests. `checkAll` passed
the same core suites plus 22 JVM and 20 Scala.js optional-codec tests. The
README quickstart is exercised by `ReadmeQuickstartSuite` on both platforms;
typed array, provider, read, write, JVM Path, and Browser façade suites cover
the remaining public entry points and failure/receipt boundaries.

## Clean detached release-boundary audit

To exercise the release-boundary gate without changing the project checkout, the
candidate diff and untracked implementation files were materialized in a
temporary local clone and committed only there as
`27e2ac9b7b8d18c066baab3fdf11d8698a3bf651`. The checkout was then detached at
that revision; `git status --porcelain` was empty and no branch was attached.

From that clean detached checkout, the following commands passed:

```text
npm ci --cache /private/tmp/zarr4s-z10-audit.50gL1o/npm-cache \
  --prefix codec-blosc-zstd/js
COURSIER_CACHE=/private/tmp/zarr4s-z10-audit.50gL1o/coursier \
  sbt -Dsbt.boot.directory=/private/tmp/zarr4s-z10-audit.50gL1o/sbt-boot \
      -Dsbt.global.base=/private/tmp/zarr4s-z10-audit.50gL1o/sbt-global \
      -Dsbt.ivy.home=/private/tmp/zarr4s-z10-audit.50gL1o/ivy checkAll
```

The detached audit reproduced 231 JVM and 235 Scala.js core tests plus 22 JVM
and 20 Scala.js optional-codec tests. The temporary commit was not added to
the zarr4s project history and is not a release or publication commit.

## Published-artifact consumer

The candidate was published locally with:

```text
sbt 'coreJVM/publishLocal' 'coreJS/publishLocal'
```

The timestamped snapshot emitted by sbt-typelevel was
`0.1-1da4c46-20260802T171907Z-SNAPSHOT`. Against that exact artifact, the
standalone consumer passed:

```text
cd examples/standalone-consumer
sbt -Dzarr4s.version=0.1-1da4c46-20260802T171907Z-SNAPSHOT \
  'consumerJVM/compile' 'consumerJS/compile'
sbt -Dzarr4s.version=0.1-1da4c46-20260802T171907Z-SNAPSHOT \
  'consumerJVM/runMain example.StandaloneConsumerMain' 'consumerJS/run'
```

The consumer uses only the public `zarr4s.*` surface and checks the typed
quickstart values on both runtimes. The unqualified default version in the
consumer README remains useful for a timestamp-free published version; this
run records the explicit snapshot override required by the local publication.

## Independent writer courts

`core.jvm`'s `WriterFixtureMain` now emits façade-created arrays for direct v3,
border chunks, fill-only omission, indexed sharding, and v2. The fixture was
read back from `/tmp/zarr4s-z10-writer.1bkCzV` with:

```text
uvx --with 'zarr==3.2.1' --with numpy python \
  tools/verify_zarr_python_interop.py verify-scala \
  /tmp/zarr4s-z10-writer.1bkCzV
python tools/verify_zarrs_v2_interop.py /tmp/zarr4s-z10-writer.1bkCzV
```

Both passed. Zarr-Python reconstructed the direct, border, fill, sharded, and
v2 façade arrays and checked that the fill-only directory contains only
`zarr.json`. The zarrs oracle reconstructed both the existing gzip v2 fixture
and the façade v2 fixture, printing their exact logical values. Internal typed
read/write suites assert the corresponding execution/write receipt accounting;
the independent readers validate values and format rather than Scala receipt
objects.

## ScalaFIM migration probe

The real downstream `dataset-zarr` JVM test surface compiled against this
checkout:

```text
cd /Users/bbuchsbaum/code/scala/scalafim
sbt -Dscalafim.zarr4s.build=/Users/bbuchsbaum/code/scala/zarr4s \
  'datasetZarrJVM/Test/compile'
```

An additional temporary test source, compiled without modifying the ScalaFIM
checkout, replaced the interpolated descriptor and dense provider for the
bounded int16 NIfTI fixture with `Nifti.readVec`,
`ArraySpec(DType.Int16, ...)`, `DenseArray.copyOf`, and
`JvmZarr.createArray`:

```text
sbt -Dscalafim.zarr4s.build=/Users/bbuchsbaum/code/scala/zarr4s \
  'set datasetZarrJVM / Test / unmanagedSourceDirectories += file("/private/tmp/zarr4s-scalafim-probe")' \
  'datasetZarrJVM/Test/compile'
```

That probe compiled successfully. It demonstrates that typed specification and
dense ownership can replace the generic descriptor/provider boilerplate for a
bounded scalar import. It does not claim a full importer migration: the
production path is intentionally streaming, applies NIfTI calibration and
byte-order policy, and publishes a NeuroArchive group/manifest through
`JvmNeuroArchivePublisher`. Those are ScalaFIM domain responsibilities. The
remaining migration work is therefore an explicit downstream adapter (and, if
it keeps the streaming publisher boundary, a small public typed-provider
bridge); no NIfTI or NeuroArchive policy was added to zarr4s core.

## Documentation source boundary

There is no mdoc/Laika site build in this repository. The README quickstart
body is nevertheless compiled as the checked-in
`core/shared/src/test/scala/zarr4s/ReadmeQuickstartExample.scala` source, and
`ReadmeQuickstartSuite` executes that exact source on JVM and Scala.js. The raw
descriptor/provider section is intentionally a hand-written advanced example;
it documents the canonical low-level API rather than claiming generated-site
coverage.
