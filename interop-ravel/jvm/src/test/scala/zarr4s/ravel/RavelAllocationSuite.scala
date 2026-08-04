package zarr4s.ravel

class RavelAllocationSuite extends munit.FunSuite:
  test("correctness and ownership parity precede allocation comparisons"):
    assertEquals(RavelAllocationEvidence.correctness, Right(()))

  test("JVM allocation court distinguishes direct, chunk-bounded, and explicit copies"):
    val rows = RavelAllocationEvidence.rows.map(row => row.scenario -> row).toMap
    val direct = rows("direct-read-materialize")
    val dense = rows("dense-bridge-materialize")
    val canonical = rows("canonical-write-source")
    val chunk = rows("canonical-write-chunk")
    val view = rows("explicit-view-materialization")

    assert(
      direct.allocatedBytesPerOperation <= direct.logicalBytes + 64L * 1024L,
      s"direct read allocated ${direct.allocatedBytesPerOperation} bytes for ${direct.logicalBytes} logical bytes"
    )
    assert(
      dense.allocatedBytesPerOperation >= direct.allocatedBytesPerOperation * 3L,
      s"DenseArray bridge ${dense.allocatedBytesPerOperation} was not at least 3x direct ${direct.allocatedBytesPerOperation}"
    )
    assert(
      canonical.allocatedBytesPerOperation < 64L * 1024L,
      s"canonical source refinement allocated ${canonical.allocatedBytesPerOperation} bytes"
    )
    assert(
      chunk.allocatedBytesPerOperation < chunk.logicalBytes / 8L,
      s"chunk provider allocated ${chunk.allocatedBytesPerOperation} bytes against ${chunk.logicalBytes} logical bytes"
    )
    assert(
      chunk.allocatedBytesPerOperation <= chunk.chunkBytes + 64L * 1024L,
      s"chunk provider allocation ${chunk.allocatedBytesPerOperation} exceeded chunk budget ${chunk.chunkBytes}"
    )
    assert(
      view.allocatedBytesPerOperation >= view.logicalBytes,
      s"explicit view copy allocated ${view.allocatedBytesPerOperation} bytes for ${view.logicalBytes} logical bytes"
    )
