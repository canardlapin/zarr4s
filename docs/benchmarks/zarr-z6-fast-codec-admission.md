# Zarr Z6 fast-codec admission record

Date: 2026-07-20

Decision: **NoGo for 0.1; retain an external-provider research track**

This decision applies to adding Zstandard or Blosc to `zarr4s-core` 0.1. It
does not reject those codecs. It rejects coupling the dependency-free kernel to
an implementation before one provider passes the same JVM, Scala.js, bounded
decode, corruption, deployment, and Python-oracle gates as gzip.

## Why the candidate remains valuable

The Z5 canonical-BOLD measurement found Blosc/Zstandard with shuffle at a 1.569
compression ratio, 1071 MiB/s encode, and 3609 MiB/s decode on the measured
corpus, versus 1.304, 63.6, and 336.5 for gzip level 1. The performance case is
real. Blosc is also a standardized Zarr v3 `bytes -> bytes` codec, including
explicit Zstandard, shuffle, typesize, and blocksize metadata; it is not a
private NeuroArchive invention. See the [normative Zarr Blosc codec
specification](https://zarr-specs.readthedocs.io/en/latest/v3/codecs/blosc/).

## Gate assessment

| Gate | JVM Zstandard | Scala.js/browser Zstandard | Blosc framing + shuffle | Result |
| --- | --- | --- | --- | --- |
| Executable implementation | `zstd-jni` is mature and exposes compression and decompression | `fzstd` supplies a small pure-JavaScript decoder | JVM would still need a conforming Blosc container implementation or native binding | Partial |
| Symmetric read/write | Yes | No: the evaluated `fzstd` package is decompression-only | Not demonstrated on either platform in this repository | Fail |
| Bounded decoding | Can be wrapped by the Z6 expected-length contract | `fzstd` can accept an output buffer, but its documentation warns that undersizing can yield corrupt output | No corruption/size-limit suite exists yet | Fail |
| Deployment | `zstd-jni` embeds architecture-dependent native libraries; its project documents the supported platform matrix and relocation constraint | Pure JavaScript is simple to deploy, but it is an npm rather than Scala.js artifact and supplies no encoder | A native Blosc binding would add another deployment surface | Fail |
| Artifact size | Architecture-specific `zstd-jni` artifacts are hundreds of KiB and the multi-platform/cloud form is larger | `fzstd` reports about 8 KiB minified | No measured JVM + JS provider artifact budget | Not closed |
| Licensing | BSD-2-Clause | MIT | c-blosc is permissively licensed | Pass |
| Python differential | Zarr-Python/numcodecs can serve as an oracle | No Scala.js provider exists to compare | No direct/sharded/corruption matrix exists | Fail |

The JVM facts come from the [`zstd-jni` project](https://github.com/luben/zstd-jni),
which states that its produced JAR embeds the native library, publishes
architecture-dependent binaries, and cannot be relocated because native
linking depends on class names. The browser candidate facts come from
[`fzstd`](https://github.com/101arrowz/fzstd), which describes itself as an 8
KiB pure-JavaScript decompressor, documents its output-buffer hazard, and does
not offer compression. These are acceptable provider ingredients, but not a
single cross-platform capability ready for admission.

## Consequence

- Keep `bytes -> gzip -> crc32c` as the portable 0.1 chain.
- Add no Zstandard, Blosc, JNI, WASM, or npm runtime dependency to the core.
- Treat Zstandard and Blosc as separately published codec providers using the
  Z6 `CodecCapability` plus sync/async executor seam.
- Do not weaken the writer contract merely because the browser candidate can
  decode: a read-only provider must be named and packaged as such.

## Next admission action

Build a disposable `zarr4s-codec-blosc-zstd` spike, outside the core, with:

1. JVM encode/decode through a pinned Zstandard implementation;
2. browser and Node encode/decode through one audited JS or WASM implementation;
3. a conforming Blosc header, byte-shuffle, and bitshuffle implementation;
4. expected-length and maximum-output enforcement before allocation;
5. direct and sharded Python differential fixtures plus truncated, oversized,
   checksum-failing, and invalid-header cases;
6. published JVM/Scala.js artifact-size measurements and a platform deployment
   matrix.

Only that complete provider is a Go candidate. The Z6 external XOR proof shows
that it can be added without redesigning or adding a core branch.
