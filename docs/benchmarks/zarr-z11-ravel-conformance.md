# Z11 Ravel interoperability conformance evidence

Date: 2026-08-03

This court tests the optional `zarr4s-interop-ravel` adapter as a general Zarr boundary. ScalaFIM is not part of the fixture design or the dependency graph.

## Revisions and runtimes

- zarr4s base revision: `7f2c8ae0f5f1edeab4dcf7accd43e4bc3046718e` plus the uncommitted Z11 implementation under review
- Ravel revision used to publish the local `1.0.0-SNAPSHOT`: `d0f7bacfe3b750519dc49aca8fd466ef70ef24ec`
- Scala: 3.7.4; sbt: 1.11.7
- JVM test launcher: Homebrew Java 25.0.1; shell Java: OpenJDK 22
- Scala.js test runtime: Node.js 24.1.0
- Zarr-Python: 3.2.1; NumPy: 2.5.1
- zarrs: 0.23.13; rustc: 1.91.1
- independent zarrs corpus source: commit `cf8209811f5937cbe4594a7a3445b95c9d35872c`

The adapter remains unpublished while Ravel has no immutable published release. The module's production graph contains Ravel and zarr4s-core only. Optional codec tests depend on the adapter in the test configuration; the adapter does not depend on the codec artifact.

## Shared semantic court

The same interop sources and semantic tests run on JVM and Scala.js. On both platforms, `interopRavelJVM/test` and `interopRavelJS/test` passed 22 tests. Together the suites cover:

- all nine exact dtypes, including signed extrema, unsigned maxima, negative zero, infinities, and NaN payload preservation;
- scalar, empty, rank-one, rank-two, and rank-five shapes, plus overflow rejection before Ravel allocation;
- full, region, point, and factored reads through synchronous and asynchronous APIs;
- direct and indexed-sharded arrays, v2 and v3 metadata, non-divisible border chunks, missing chunks, and typed fill synthesis;
- create-only conflicts, bounded partial progress, malformed input, unsupported dtypes, and compile-time exclusion of mutable or borrowed Ravel storage;
- exact materialization of the attributed zarrs UInt16 shard corpus on both JVM and Node.js.

A browser smoke was not added: the optional WebAssembly codec already has its own browser-runtime suite, while the adapter itself has no browser-only transport or representation branch. Node.js exercises the Scala.js representation and asynchronous reader/writer implementation used by the browser facade.

Command:

```text
sbt -batch interopRavelJVM/test interopRavelJS/test
```

## Independent bidirectional courts

The filesystem court used `/tmp/zarr4s-z11-conformance.E4jZi6` for this run. It is reproducible with a fresh temporary directory:

```text
sbt -batch \
  "coreJVM/Test/runMain zarr4s.WriterFixtureMain <root>" \
  "interopRavelJVM/Test/runMain zarr4s.ravel.RavelInteropFixtureMain write-ravel <root>"
uvx --with zarr==3.2.1 --with numpy python \
  tools/verify_zarr_python_interop.py write-python <root>
uvx --with zarr==3.2.1 --with numpy python \
  tools/verify_zarr_python_interop.py verify-scala <root>
sbt -batch \
  "interopRavelJVM/Test/runMain zarr4s.ravel.RavelInteropFixtureMain verify-python <root>"
python3 tools/verify_zarrs_v2_interop.py <root>
```

All commands passed. Zarr-Python reconstructed adapter-produced arrays for every exact dtype, including raw-bit checks for floating negative zero and NaN, plus border, indexed-sharded, and v2 arrays. The adapter reconstructed all nine compatible Zarr-Python dtype fixtures. The pinned zarrs oracle reconstructed the adapter-produced v2 Int16 fixture as `[1, -2, 300, 4, 5, -6]`.

## Optional Blosc/Zstandard court

The installed optional codec runtimes wrote a canonical Ravel Float32 source through the adapter's typed chunk-provider seam, then reopened and materialized it as a bitwise-equal Ravel array. This passed on the JVM JNI provider and the Scala.js pinned `numcodecs` WebAssembly provider:

```text
sbt -batch codecBloscZstdJVM/test codecBloscZstdJS/test
```

The JVM provider passed 23 tests and the Scala.js provider passed 21 tests. The Scala.js qualification remains explicit: its Blosc encoder supports the observed four-byte type size, while incompatible type sizes remain typed failures.

## Fixture hashes

SHA-256 hashes from the passing run:

```text
c79a414d05c38a7e489c3e333aadb484a8735dbc6011911e7ae5bd7d6b170500  tools/verify_zarr_python_interop.py
1dd5ea5ee4e63daffcba77a1741b2a3dc7e00ace0b1eee5afdfce52f1047ac02  tools/verify_zarrs_v2_interop.py
eaf16facb9b16e6aeb2ad3c795b9b76c5e6ea47c9dd0e941c00610a60e3d9214  core/shared/src/test/scala/zarr4s/ZarrsFixtures.scala
8263772e17a38bd6536d60d5d0130b0bdd77e44f2e0fa04f8e1ee34c1ba80170  ravel-v2-int16.zarr/.zarray
d7aad1b09aa52b270ececfc54c2d98cd1fcb7572b284e7d12d8211c501812988  ravel-v2-int16.zarr/0.0
daaa42295f30d6500ee566c29a95d616562b7bd49ef3249e170cf0f5be671063  ravel-sharded.zarr/zarr.json
b9b1d40cee6738806f0ab6d16f4ad7291d923e57a501971e0871d5f1cfd01ffc  ravel-sharded.zarr/c/0/0
38fab0bc2e83a302b5da05f6eac570eb9b3531dd7e620dd113cdb1c4eed16fcd  ravel-float32.zarr/zarr.json
fdfa6aec8aef2933028bc1ec52866db942c67ef32d1ab4e1e9e9121d1a4bd1ae  ravel-float32.zarr/c/0/0
cf1454d014da69217281c95d94eb4f6b6d0f9f2ff057446d3d7bd95a1c5fb688  python-int16.zarr/zarr.json
b920dbb440025183e62d476ee850921e5cea2a1c5153b5bb502ce8a4bf6bcdaa  python-int16.zarr/c/0/0
```

These hashes identify the scripts, attributed static corpus, and representative metadata/chunk payloads. Generated filesystem fixtures are court outputs, not new checked-in binary fixtures.

## Result

Z11.5 passes. The adapter preserves the tested Zarr and Ravel semantics across formats, layouts, selections, exact dtypes, JVM, and Scala.js. It delegates codec and storage behavior to zarr4s and retains typed `ZarrError` or `RavelInteropError` failures; no fallback guesses around unsupported metadata or dtypes were introduced.
