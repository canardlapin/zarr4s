# Zarr Z6 executable codec architecture

Z6 turns codec execution into an explicit capability of the generic Zarr
kernel. Metadata compilation is already extensible, but execution is not: the
synchronous reader accepts a gzip-specific argument, the browser reader calls
the browser gzip implementation directly, and the JVM writer owns another
closed codec match. That split is the next extraction blocker.

This slice keeps `zarr` dependency-free and cross-platform. It does not add a
compressor dependency or invent compression algorithms.

## Public contract

Metadata and execution have different responsibilities:

```scala
trait CodecCapability:
  def name: String
  def compile(
      extension: ExtensionMetadata,
      dataType: DataTypeCapability
  ): Either[String, CompiledCodec]

trait SyncByteCodecExecutor:
  def name: String
  def decode(
      codec: CompiledCodec,
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits
  ): Either[CodecError, OwnedBytes]
  def encode(
      codec: CompiledCodec,
      decoded: OwnedBytes
  ): Either[CodecError, OwnedBytes]

trait AsyncByteCodecExecutor:
  def name: String
  def decode(
      codec: CompiledCodec,
      encoded: OwnedBytes,
      expectedDecoded: ByteCount,
      limits: DecodeLimits
  ): Future[Either[CodecError, OwnedBytes]]
```

`CodecCapability` understands JSON configuration. An executor supplies an
algorithm for a compiled bytes-to-bytes stage. A `CodecProgram` is the lawful,
immutable bridge between them. It records the initial representation, ordered
stages, final representation, and required executor names. Invalid stage
transitions cannot inhabit a descriptor.

`CodecProgram` remains runtime-rank. A codec may inspect the compiled scalar
type and expected decoded byte count, but the kernel does not encode rank in a
match type or force an effect abstraction over both platforms.

The two runtime families are deliberately separate:

- `SyncCodecRuntime` is used by synchronous readers and the JVM writer.
- `AsyncCodecRuntime` is used by the Scala.js reader.

Both are immutable registries with duplicate-name validation. Opening an array
checks that its compiled program is executable by the supplied runtime. A
known metadata codec with no executor is a typed missing-capability error, not
a late match failure.

## Program invariants

- Direct and inner-chunk programs start with array values and end with bytes.
- An array-to-bytes stage occurs exactly once, after zero or more lawful
  array-to-array stages and before every bytes-to-bytes stage. The 0.1
  interpreters deliberately reject array-to-array execution until a provider
  contract for typed array values is specified.
- Outer shard programs start and end with bytes; the empty identity program is
  lawful even though bounded readers still reject non-empty outer transforms.
- The promoted shard-index program is exactly little-endian uint64 bytes
  followed by CRC32C. It is represented explicitly even while that is the only
  executable index profile.
- Every bytes-to-bytes stage names one executor requirement.
- The expected decoded scalar byte length is checked arithmetically before any
  codec executes and remains bounded by `DecodeLimits`.
- Each stage owns its returned `OwnedBytes`; no platform buffer escapes.
- Encoding checks the configured maximum after every stage.
- Unknown `must_understand = false` codecs remain ignorable at compilation.
  Unknown required schemas and known schemas with absent executors are distinct
  failures.

## Built-ins and extension seam

The shared artifact owns bytes layout and CRC32C. JVM and browser gzip are
adapters behind the runtime contracts. The built-in portable runtimes contain
only these providers.

Z6 proves the seam using a test-only reversible bytes codec. Its metadata
schema and JVM/Scala.js executors live outside the built-in implementation.
Direct and sharded fixtures must open and round-trip without edits to metadata
parsing, planning, reading, writing, or NeuroArchive code.

Zstandard or Blosc is admitted only as a separately packaged provider after a
candidate passes JVM and Scala.js availability, artifact-size, licensing,
deployment, Python differential, corruption, and bounded-decoding gates. A
NoGo decision is an acceptable Z6 result; adding a heavy dependency to `zarr`
is not.

## Migration

- Replace `SyncGzipCapability` with `SyncCodecRuntime` at synchronous open
  boundaries.
- Supply the dependency-free JVM portable runtime by default to the JVM writer.
- Replace browser hard-coding with an explicit `AsyncCodecRuntime`, defaulting
  to the browser portable runtime.
- Store compiled programs, rather than unchecked codec vectors, in
  `PhysicalLayout`.
- Render any compiled codec from its canonical name and configuration so an
  external provider can participate in create-only writing.
- Keep NeuroArchive as a client. Its profile continues to restrict the lawful
  generic surface to little-endian bytes, gzip level 1, and CRC32C.

## Completion evidence

1. Shared program-law tests run on JVM and Scala.js.
2. Existing direct, start-indexed, and end-indexed fixtures remain green.
3. JVM create-only output remains readable by Zarr-Python.
4. The external codec fixture succeeds on JVM and Scala.js and fails precisely
   when either schema or executor support is absent.
5. Minimal standalone JVM and Scala.js consumers compile against `zarr` alone.
6. The artifact dependency graph remains free of runtime libraries.
7. A fast-codec admission record names a Go or NoGo outcome.
8. `sbt testAll` is warning-clean.
