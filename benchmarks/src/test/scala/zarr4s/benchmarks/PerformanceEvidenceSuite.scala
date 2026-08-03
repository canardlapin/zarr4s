package zarr4s.benchmarks

import scala.io.Source

class PerformanceEvidenceSuite extends munit.FunSuite:
  private def value[A](result: Either[String, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error)

  test("versioned performance receipt matches executable evidence"):
    val source = Source.fromResource("advanced/performance-evidence.csv")
    val expected =
      try source.mkString
      finally source.close()
    assertEquals(value(PerformanceEvidence.csv), expected)

  test("evidence rows preserve accounting and cache laws"):
    val rows = value(PerformanceEvidence.rows)
    rows.foreach: row =>
      assertEquals(row.indexBytesRead + row.dataBytesRead, row.bytesRead)
      assertEqualsDouble(
        row.readAmplification,
        row.bytesRead.toDouble / row.logicalBytes.toDouble,
        1e-12
      )
    val warm = rows.filter(_.phase == "warm")
    assertEquals(warm.map(_.downstreamRequests), Vector(0L, 0L))
    assertEquals(warm.map(_.fetchedBytes), Vector(0L, 0L))
    assert(warm.forall(_.cacheHits > 0L))

  test("published guide tables match executable evidence"):
    assertEvidenceTable(
      "advanced/choosing-chunks.md",
      "chunks",
      PerformanceEvidence.chunkGuideTable
    )
    assertEvidenceTable(
      "advanced/choosing-shards.md",
      "shards",
      PerformanceEvidence.shardGuideTable
    )
    assertEvidenceTable(
      "advanced/remote-performance.md",
      "cache",
      PerformanceEvidence.cacheGuideTable
    )

  private def assertEvidenceTable(
      resource: String,
      name: String,
      generated: Either[String, String]
  ): Unit =
    val source = Source.fromResource(resource)
    val page =
      try source.mkString
      finally source.close()
    val start = s"<!-- evidence:$name:start -->"
    val end = s"<!-- evidence:$name:end -->"
    val startIndex = page.indexOf(start)
    val endIndex = page.indexOf(end)
    assert(startIndex >= 0, s"missing $start in $resource")
    assert(endIndex > startIndex, s"missing $end in $resource")
    val published = page.substring(startIndex + start.length, endIndex).trim
    assertEquals(published, value(generated))
