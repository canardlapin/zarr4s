package zarr4s

class RankFourRemoteCacheSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def bytes(value: String): OwnedBytes =
    OwnedBytes.copyOf(value.iterator.map(_.toByte).toArray)

  test("rank-four reread eliminates downstream object and byte transfer"):
    val metadata =
      """{"zarr_format":3,"node_type":"array","shape":[96,96,72,1200],"data_type":"int16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[32,32,24,16]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}],"dimension_names":["x","y","z","time"],"attributes":{},"storage_transformers":[]}"""
    val encodedChunkBytes = 32L * 32L * 24L * 16L * 2L
    val payload = OwnedBytes.copyOf(new Array[Byte](encodedChunkBytes.toInt))
    val objects = scala.collection.mutable.Map[String, OwnedBytes](
      "zarr.json" -> bytes(metadata)
    )
    var x = 0
    while x <= 1 do
      var y = 0
      while y <= 1 do
        var z = 0
        while z <= 1 do
          var time = 25
          while time <= 26 do
            objects += s"c/$x/$y/$z/$time" -> payload
            time += 1
          z += 1
        y += 1
      x += 1

    val downstream = zvalue(MemoryStore(objects.toMap))
    val cache = ObjectReadCache(
      zvalue(CacheNamespace.from("fmri-run@fixture-v1")),
      CacheLimits(32, zvalue(ByteCount(32L * 1024L * 1024L)))
    )
    val opened = zvalue(SyncZarr.openArray(CachingObjectReader(downstream, cache)))
    val region = zvalue(
      Region.within(
        opened.descriptor.shape,
        zvalue(Coordinate(16L, 16L, 12L, 400L)),
        zvalue(Shape(32L, 32L, 24L, 32L))
      )
    )

    downstream.clearTrace()
    val before = cache.stats
    val first = zvalue(opened.readRegion(region))
    val afterFirst = cache.stats
    val firstTrace = downstream.trace
    val second = zvalue(opened.readRegion(region))
    val afterSecond = cache.stats

    val fetched = encodedChunkBytes * 16L
    assertEquals(first.receipt.objectRequests, 16)
    assertEquals(first.receipt.bytesRead, fetched)
    assertEquals(first.receipt.requestedLogicalBytes, 32L * 32L * 24L * 32L * 2L)
    assertEqualsDouble(first.receipt.readAmplification, 8.0, 1e-12)
    assertEquals(firstTrace.length, 16)
    assertEquals(afterFirst.downstreamRequests - before.downstreamRequests, 16L)
    assertEquals(afterFirst.fetchedBytes - before.fetchedBytes, fetched)
    (first.block, second.block) match
      case (PrimitiveBlock.Int16(left), PrimitiveBlock.Int16(right)) =>
        assert(java.util.Arrays.equals(left.values, right.values))
      case _ => fail("expected int16 results")
    assertEquals(downstream.trace, firstTrace)
    assertEquals(afterSecond.downstreamRequests - afterFirst.downstreamRequests, 0L)
    assertEquals(afterSecond.fetchedBytes - afterFirst.fetchedBytes, 0L)
    assertEquals(afterSecond.hits - afterFirst.hits, 16L)
    assertEquals(afterSecond.servedBytes - afterFirst.servedBytes, fetched)
