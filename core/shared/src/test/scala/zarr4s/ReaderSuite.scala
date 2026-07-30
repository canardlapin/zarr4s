package zarr4s

class ReaderSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def cvalue[A](result: Either[CodecError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def bytes(value: String): OwnedBytes =
    OwnedBytes.copyOf(value.iterator.map(_.toByte).toArray)

  private def int16(values: Short*): OwnedBytes =
    cvalue(
      ScalarBytes.encode(
        PrimitiveBlock.Int16(OwnedShorts.copyOf(values.toArray)),
        BuiltInDataTypes.int16,
        Some(Endianness.Little)
      )
    )

  private def shorts(result: ReadResult): Vector[Short] = result.block match
    case PrimitiveBlock.Int16(values) => values.toArray.toVector
    case _                            => fail("expected int16 result")

  private val directMetadata =
    """{"zarr_format":3,"node_type":"array","shape":[4,5],"data_type":"int16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,3]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":-9,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}],"dimension_names":["y","x"],"attributes":{},"storage_transformers":[]}"""

  private def directStore: MemoryStore = zvalue(
    MemoryStore(
      Map(
        "zarr.json" -> bytes(directMetadata),
        "c/0/0" -> int16(0, 1, 2, 5, 6, 7),
        "c/0/1" -> int16(3, 4, -9, 8, 9, -9),
        "c/1/1" -> int16(13, 14, -9, 18, 19, -9)
      )
    )
  )

  test("generic direct reader assembles full border chunks and missing fill"):
    val store = directStore
    val opened = zvalue(SyncZarr.openArray(store))
    store.clearTrace()
    val region = zvalue(
      Region.within(
        opened.descriptor.shape,
        zvalue(Coordinate(1L, 1L)),
        zvalue(Shape(3L, 4L))
      )
    )
    val result = zvalue(opened.readRegion(region))
    assertEquals(
      shorts(result),
      Vector[Short](
        6, 7, 8, 9, -9, -9, 13, 14, -9, -9, 18, 19
      )
    )
    assertEquals(result.shape, zvalue(Shape(3L, 4L)))
    assertEquals(result.receipt.objectRequests, 4)
    assertEquals(store.trace.length, 4)

  test("generic point reader preserves order and duplicates across fill and edge chunks"):
    val store = directStore
    val opened = zvalue(SyncZarr.openArray(store))
    store.clearTrace()
    val points = zvalue(
      CoordinateBatch.within(
        opened.descriptor.shape,
        Seq(
          zvalue(Coordinate(3L, 4L)),
          zvalue(Coordinate(0L, 0L)),
          zvalue(Coordinate(2L, 1L)),
          zvalue(Coordinate(3L, 4L))
        )
      )
    )
    val result = zvalue(opened.readPoints(points))
    assertEquals(shorts(result), Vector[Short](19, 0, -9, 19))
    assertEquals(result.shape, zvalue(Shape(4L)))
    assertEquals(result.receipt.touchedChunks, 3)

  test("factored reader materializes ordered per-axis slices and duplicate gathers"):
    val store = directStore
    val opened = zvalue(SyncZarr.openArray(store))
    val selection = zvalue(
      FactoredSelection.within(
        opened.descriptor.shape,
        Vector(
          AxisSelector.Indices(zvalue(AxisIndices.from(Vector(3L, 1L, 3L)))),
          AxisSelector.Slice(zvalue(AxisSlice(0L, 5L, 2L)))
        )
      )
    )
    store.clearTrace()
    val result = zvalue(opened.read(selection))
    assertEquals(result.shape, zvalue(Shape(3L, 3L)))
    assertEquals(
      shorts(result),
      Vector[Short](
        -9, -9, 19, 5, 7, 9, -9, -9, 19
      )
    )
    assertEquals(result.receipt.touchedChunks, 4)

  test("start-indexed Python shard is read in two bounded phases"):
    val store = zvalue(
      MemoryStore(
        Map(
          "zarr.json" -> bytes(ZarrBinaryFixtures.shardedStartMetadata),
          "c/0/0" -> ZarrBinaryFixtures.shardedStartObject
        )
      )
    )
    val opened = zvalue(SyncZarr.openArray(store))
    store.clearTrace()
    val region = zvalue(
      Region.within(
        opened.descriptor.shape,
        zvalue(Coordinate(0L, 0L)),
        opened.descriptor.shape
      )
    )
    val result = zvalue(opened.readRegion(region))
    assertEquals(
      shorts(result),
      Vector[Short](
        1, 2, 0, 0, 3, 4, 0, 0, 0, 0, 13, 14, 0, 0, 15, 16
      )
    )
    assertEquals(result.receipt.objectRequests, 2)
    assertEquals(result.receipt.rangeRequests, 2)
    assertEquals(result.receipt.lengthRequests, 0)
    assertEquals(result.receipt.bytesRead, 84L)
    assertEquals(result.receipt.indexBytesRead, 68L)
    assertEquals(result.receipt.dataBytesRead, 16L)
    assertEquals(result.receipt.requestedLogicalBytes, 32L)
    assertEqualsDouble(result.receipt.readAmplification, 2.625, 1e-12)
    assertEquals(store.trace.collect { case request: ObjectRequest.Range => request }.length, 2)

  test("factored selections flow through indexed shards without point expansion"):
    val store = zvalue(
      MemoryStore(
        Map(
          "zarr.json" -> bytes(ZarrBinaryFixtures.shardedStartMetadata),
          "c/0/0" -> ZarrBinaryFixtures.shardedStartObject
        )
      )
    )
    val opened = zvalue(SyncZarr.openArray(store))
    val selection = zvalue(
      FactoredSelection.within(
        opened.descriptor.shape,
        Vector(
          AxisSelector.Indices(zvalue(AxisIndices.from(Vector(3L, 0L)))),
          AxisSelector.Indices(zvalue(AxisIndices.from(Vector(3L, 0L, 3L))))
        )
      )
    )
    val result = zvalue(opened.read(selection))
    assertEquals(result.shape, zvalue(Shape(2L, 3L)))
    assertEquals(shorts(result), Vector[Short](16, 0, 16, 0, 1, 0))

  test("empty region performs no payload store access"):
    val store = directStore
    val opened = zvalue(SyncZarr.openArray(store))
    store.clearTrace()
    val region = zvalue(
      Region.within(
        opened.descriptor.shape,
        zvalue(Coordinate(4L, 0L)),
        zvalue(Shape(0L, 5L))
      )
    )
    val result = zvalue(opened.readRegion(region))
    assertEquals(result.block.elementCount, 0)
    assertEquals(store.trace, Vector.empty)

  test("end-indexed shards execute when the store supplies checked object length"):
    val store = zvalue(
      MemoryStore(
        Map(
          "zarr.json" -> bytes(ZarrBinaryFixtures.shardedEndMetadata),
          "c/0/0" -> ZarrBinaryFixtures.shardedEndObject
        )
      )
    )
    val opened = zvalue(SyncZarr.openArray(store))
    val region = zvalue(
      Region.within(
        opened.descriptor.shape,
        zvalue(Coordinate(0L, 0L)),
        opened.descriptor.shape
      )
    )
    store.clearTrace()
    val result = zvalue(opened.readRegion(region))
    assertEquals(
      shorts(result),
      Vector[Short](
        1, 2, 0, 0, 3, 4, 0, 0, 0, 0, 13, 14, 0, 0, 15, 16
      )
    )
    assertEquals(result.receipt.objectRequests, 3)
    assertEquals(result.receipt.rangeRequests, 2)
    assertEquals(result.receipt.lengthRequests, 1)
    assertEquals(result.receipt.indexBytesRead, 68L)
    assertEquals(
      store.trace,
      Vector(
        ObjectRequest.Length(StoreKey.unsafe("c/0/0")),
        ObjectRequest.Range(StoreKey.unsafe("c/0/0"), zvalue(ByteRange(16L, 68L))),
        ObjectRequest.Range(StoreKey.unsafe("c/0/0"), zvalue(ByteRange(0L, 16L)))
      )
    )

  test("open enforces decoded chunk limits before payload access"):
    val store = directStore
    val limits = OpenLimits(maxDecodedChunkBytes = zvalue(ByteCount(8L)))
    assert(SyncZarr.openArray(store, limits = limits).isLeft)
