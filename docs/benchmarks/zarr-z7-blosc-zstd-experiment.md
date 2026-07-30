# Zarr Z7 Blosc/Zstandard provider experiment

Date: 2026-07-20

Decision: **Go as an optional fMRI-oriented provider; NoGo as a core dependency
or general Scala.js writer**

## Question

Can the Z6 codec seam support standards-conforming Blosc/Zstandard without
making the dependency-free `zarr4s-core` kernel depend on JNI, WebAssembly,
or npm?

Yes. The experiment is implemented as the separate
`zarr4s-codec-blosc-zstd` cross-project. Its shared code compiles
normative Zarr v3 Blosc metadata into typed values and checks the 16-byte Blosc
frame before any native or WebAssembly decoder can allocate. Platform code is
only the algorithm interpreter:

| Surface | JVM | Scala.js |
| --- | --- | --- |
| Decode | `blosc-java` JNI | `numcodecs` embedded WASM |
| Encode | All admitted `typesize` values | Exactly `typesize == 4` |
| Shuffle | none, byte, bit | none, byte, bit |
| Direct Zarr v3 | read/write | read; executor-level write |
| Indexed sharding | read/write | read |
| Core dependency added | none | none |

The Scala.js encoder restriction is deliberate. `numcodecs` 0.3.2 does not
expose Blosc's `typesize` argument and the produced frame records a fixed value
of four. The provider verifies the output frame and rejects every other
requested stride before writing. It does not silently relabel the bytes.

## Oracle and failure evidence

The checked-in fixtures were produced by Zarr-Python 3.2.1 and numcodecs
0.16.3. Both JVM and Scala.js reconstruct:

- a direct float32 array with byte shuffle and `typesize = 4`;
- a float32 `sharding_indexed` object with four independently compressed inner
  chunks;
- a direct int16 array with byte shuffle and `typesize = 2`, demonstrating that
  the Scala.js limitation is write-only.

In the other direction, `BloscWriterFixtureMain` writes direct and sharded
float32 arrays through the real `JvmZarrWriter`; Zarr-Python reconstructs both
exactly. The reproducible gate is:

```text
npm ci --prefix codec-blosc-zstd/js
uv run --with 'zarr==3.2.1' --with numpy \
  python tools/verify_zarr_blosc_interop.py write-python <root>
sbt 'codecBloscZstdJVM/Test/runMain \
  zarr4s.codec.blosc.BloscWriterFixtureMain <root>'
uv run --with 'zarr==3.2.1' --with numpy \
  python tools/verify_zarr_blosc_interop.py verify-scala <root>
```

The platform suites also cover truncated headers, forged decoded lengths,
metadata/frame `typesize` disagreement, maximum decoded-size enforcement, and
all three shuffle modes. These checks occur before JNI/WASM invocation. They do
not replace an outer CRC32C codec when silent payload corruption detection is a
requirement.

## Dependency cost

Measured from the resolved artifacts on 2026-07-20:

| Artifact | Raw/unpacked bytes | gzip bytes | Consequence |
| --- | ---: | ---: | --- |
| `blosc-java-0.3-1.21.6.jar` | 2,058,408 | 2,053,622 | Contains native libraries for several OS/architectures; almost no transfer compression gain. |
| `numcodecs/dist/blosc.js` | 614,996 | 206,025 | Includes the WASM binary as base64 in one ESM file. |
| Complete `numcodecs` package | 1,428,815 | not measured | Installed development footprint; a subpath import avoids loading unrelated codecs at runtime. |

The JVM dependency is too heavy and operationally specific for the Zarr core.
The browser payload is defensible for applications that actually need Blosc,
but not for every Scalafim user. Optional publication is therefore the correct
boundary.

## Admission decision

Admit the module as an experimental optional provider for the current
neuroimaging path:

- JVM can write compact int16 or float32 arrays and read them on demand;
- Scala.js can read both int16 and float32 Zarr-Python data;
- Scala.js can write the common float32/`typesize = 4` case;
- direct and sharded representations use the same core planner and stores.

Do not make it the default codec, promise a general Scala.js Blosc writer, or
fold either dependency into `zarr4s-core`. A full standalone `zarr4s`
provider should either own a small conforming Blosc encoder or use a maintained
browser binding that exposes `typesize`. Browser bundler execution is also
still a release gate: this experiment ran the Scala.js suite under Node, not in
Chromium, Firefox, or Safari.
