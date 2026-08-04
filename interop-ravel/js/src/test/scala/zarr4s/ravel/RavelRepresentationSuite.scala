package zarr4s.ravel

class RavelRepresentationSuite extends munit.FunSuite:
  test("Scala.js evidence retains exact canonical typed-buffer behavior"):
    val rows = RavelRepresentationEvidence.rows.map(row => row.scenario -> row).toMap
    val direct = rows("direct-read-materialize")
    val dense = rows("dense-bridge-materialize")
    val canonical = rows("canonical-write-source")
    val chunk = rows("canonical-write-chunk")
    val view = rows("explicit-view-materialization")

    assert(rows.values.forall(row => row.canonical && row.wholeBuffer && row.exactDType))
    assert(direct.arrayBufferDeltaBytes >= direct.logicalBytes)
    assert(dense.arrayBufferDeltaBytes > direct.arrayBufferDeltaBytes)
    assert(canonical.arrayBufferDeltaBytes < canonical.chunkBytes.max(1024L))
    assert(chunk.arrayBufferDeltaBytes < chunk.logicalBytes)
    assert(view.arrayBufferDeltaBytes >= view.logicalBytes)
