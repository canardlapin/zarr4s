# Z11 Ravel interoperability allocation evidence

Date: 2026-08-03

This court isolates the adapter boundary after Zarr decoding. Correctness and ownership parity run before allocation assertions. It does not substitute timing for format conformance, which is recorded separately in `zarr-z11-ravel-conformance.md`.

## Workload and method

- dtype: Int32
- shape: 512 by 512, 262,144 elements, 1,048,576 logical bytes
- chunk shape: 64 by 64, 16,384 bytes
- codec: none in this isolated materialization court
- zarr4s candidate revision: `dcffada0188ac35c30ebe93b74cf862d1e9f04b9`
- Ravel revision: `d0f7bacfe3b750519dc49aca8fd466ef70ef24ec`
- host: Darwin 23.3.0, arm64; JVM launcher: Homebrew Java 25.0.1; Node.js 24.1.0

The JVM court uses `com.sun.management.ThreadMXBean` for thread allocation bytes and heap memory-pool peak counters. It runs sbt with escape analysis disabled so a successful result cannot disappear through scalar replacement. Each row records its own warmup and measurement count. Peak heap delta is a sampled pool counter: a zero for the small source/chunk cases means the operation stayed below the counter's observable baseline, not that no objects were allocated. Thread allocation bytes are the budget authority for those cases.

The Scala.js court records Node's `process.memoryUsage().arrayBuffers` before and after retaining each result. This is representation-sensitive: Ravel's Int32 output is a JavaScript typed buffer. The Node counter can under-resolve the 16 KiB chunk case, so the JavaScript chunk row is structural evidence only; the JVM allocation count and exact Int32 block assertion are the chunk budget court.

## Correctness and ownership controls

Before measurement, the executable court establishes that:

- direct PrimitiveBlock-to-NDArray materialization and the legacy DenseArray bridge are bitwise equal to the same canonical Ravel input;
- `RavelArraySource.fromCanonical` retains the original immutable Ravel owner by reference;
- `RavelArraySource.copyOf` for a view produces a distinct canonical owner;
- every Scala.js output is canonical, whole-buffer, and carries the exact Ravel Int32 dtype.

## JVM raw receipt

Command:

```text
sbt -J-XX:-DoEscapeAnalysis -batch \
  interopRavelJVM/test \
  "interopRavelJVM/Test/runMain zarr4s.ravel.RavelAllocationEvidence"
```

Raw CSV:

```text
scenario,logical_bytes,chunk_bytes,warmup_iterations,measurement_iterations,allocated_bytes_per_operation,peak_heap_delta_bytes,nanoseconds_per_operation,throughput_mib_per_second,checksum
direct-read-materialize,1048576,0,5,20,1052064,2097152,196512,5088.735,0
dense-bridge-materialize,1048576,0,2,8,9439872,15728640,1010656,989.456,262144
canonical-write-source,1048576,0,100,1000,376,0,3673,272241.594,262144
canonical-write-chunk,1048576,16384,20,200,17376,0,121782,8211.360,4096
explicit-view-materialization,1048576,0,2,8,1052124,2097152,384114,2603.390,262144
```

The direct read allocated 1,052,064 bytes: one 1,048,576-byte Ravel output buffer plus 3,488 bytes of shape, owner, and measurement-visible scaffolding beyond the already decoded PrimitiveBlock. The DenseArray bridge allocated 9,439,872 bytes, about nine times the direct path, because its safe ownership boundaries and Ravel construction require multiple whole-buffer copies and sequence adaptation.

Canonical source refinement allocated 376 bytes, independent of the 1 MiB source payload. Producing one canonical write chunk allocated 17,376 bytes, only 992 bytes beyond its 16,384-byte nominal chunk buffer and far below the whole-array size. The first court run exposed a successful `Either` allocation per copied element; changing successful scalar copies to the allocation-free `None` case reduced the measured chunk operation from 82,912 to 17,376 bytes while retaining typed mismatch errors.

The explicit view path allocated 1,052,124 bytes and is reported separately. That cost is
intentional canonical materialization and must not be folded into claims about canonical writes.

The sampled peak heap deltas were 2 MiB for direct materialization, 15 MiB for the DenseArray
bridge, and 2 MiB for explicit view materialization. Source refinement and the 16 KiB chunk did not
rise above the memory-pool counter baseline. Their retained source-side storage bounds are
established directly by ownership and allocation receipts: no new full source buffer for canonical
refinement and one nominal chunk buffer for chunk production.

## Scala.js raw receipt

The test gate is:

```text
sbt -batch interopRavelJS/test
```

The standalone receipt uses an in-memory linker setting so the normal MUnit test initializer remains unchanged:

```text
sbt -batch \
  "set interopRavelJS / Test / scalaJSUseTestModuleInitializer := false" \
  "set interopRavelJS / Test / scalaJSUseMainModuleInitializer := true" \
  "set interopRavelJS / Test / mainClass := Some(\"zarr4s.ravel.RavelRepresentationEvidence\")" \
  "interopRavelJS / Test / run"
```

Raw CSV:

```text
scenario,logical_bytes,chunk_bytes,array_buffer_delta_bytes,elapsed_milliseconds,canonical,whole_buffer,exact_dtype
direct-read-materialize,1048576,0,1048600,3,true,true,true
dense-bridge-materialize,1048576,0,5243152,7,true,true,true
canonical-write-source,1048576,0,48,0,true,true,true
canonical-write-chunk,1048576,16384,16400,2,true,true,true
explicit-view-materialization,1048576,0,1048656,3,true,true,true
```

The direct Scala.js path added 1,048,600 array-buffer bytes, 24 bytes beyond one logical Int32 output
buffer. The DenseArray bridge added 5,243,152 bytes, roughly five logical buffers. Canonical source
refinement added only 48 rank/metadata bytes, the chunk operation added its 16,384-byte typed buffer
plus 16 bytes, and explicit view materialization added one full output buffer plus 80 bytes. The JVM
thread-allocation count remains the chunk budget authority because Node's counter can vary with
allocator reuse.

## Interpretation

The evidence supports these narrow claims:

- a native read adds one Ravel output buffer beyond the decoded zarr4s block;
- a canonical write does not copy the whole source array;
- canonical write-side storage and measured allocation are bounded by chunk size, not array size;
- view materialization is explicit and costs one whole Ravel output buffer;
- the legacy DenseArray bridge is materially more allocation-heavy for this workload.

The timing columns describe only this local, uncompressed in-memory adapter operation. They are not end-to-end Zarr throughput, a universal speedup, or a browser-performance claim. No browser timing was run.

Evidence-source SHA-256 hashes:

```text
30032b5f2bbb292e58e16f14e6694d4787d3035bc37e5310bd21fa9763efa79a  RavelAllocationEvidence.scala
85fffcad1eb6a57b660bd4b76c18869464569a024664894217f5ef160f71ed56  RavelRepresentationEvidence.scala
```
