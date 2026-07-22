package scalafim.zarr

class FragmentSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error) => fail(error.message)

  private def bytes(value: String): OwnedBytes =
    OwnedBytes.copyOf(value.iterator.map(_.toByte).toArray)

  private def int16(values: Short*): OwnedBytes =
    ScalarBytes.encode(
      PrimitiveBlock.Int16(OwnedShorts.copyOf(values.toArray)),
      BuiltInDataTypes.int16,
      Some(Endianness.Little)
    ) match
      case Right(found) => found
      case Left(error) => fail(error.message)

  private val directMetadata =
    """{"zarr_format":3,"node_type":"array","shape":[4,5],"data_type":"int16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,3]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":-9,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}],"dimension_names":["y","x"],"attributes":{},"storage_transformers":[]}"""

  private def directStore: MemoryStore = zvalue(MemoryStore(Map(
    "zarr.json" -> bytes(directMetadata),
    "c/0/0" -> int16(0, 1, 2, 5, 6, 7),
    "c/0/1" -> int16(3, 4, -9, 8, 9, -9),
    "c/1/1" -> int16(13, 14, -9, 18, 19, -9)
  )))

  private def selection(shape: Shape): FactoredSelection = zvalue(FactoredSelection.within(
    shape,
    Vector(
      AxisSelector.Indices(zvalue(AxisIndices.from(Vector(3L, 1L, 3L)))),
      AxisSelector.Slice(zvalue(AxisSlice(0L, 5L, 2L)))
    )
  ))

  private def assemble(
      fragments: Vector[ChunkFragment],
      outputShape: Shape
  ): Vector[Short] =
    val count = zvalue(outputShape.elementCount)
    val output = Array.fill(count.toInt)(Short.MinValue)
    fragments.foreach: fragment =>
      val values = fragment.values match
        case PrimitiveBlock.Int16(found) => found.toArray
        case _ => fail("expected int16 fragment")
      val cursor = new Array[Long](fragment.shape.rank.toInt)
      var element = 0
      while element < values.length do
        var destination = 0L
        var axis = 0
        while axis < cursor.length do
          val outputIndex = zvalue(zvalue(fragment.placement.axis(axis)).outputIndex(cursor(axis)))
          destination = destination * outputShape.axis(axis) + outputIndex
          axis += 1
        output(destination.toInt) = values(element)
        advance(cursor, fragment.shape)
        element += 1
    output.toVector

  private def advance(cursor: Array[Long], shape: Shape): Unit =
    var axis = cursor.length - 1
    var advanced = false
    while axis >= 0 && !advanced do
      cursor(axis) += 1L
      if cursor(axis) < shape.axis(axis) then advanced = true
      else
        cursor(axis) = 0L
        axis -= 1

  test("direct fragment fold emits compact decoded and fill pieces with exact placement"):
    val store = directStore
    val opened = zvalue(SyncZarr.openArray(store))
    val selected = selection(opened.descriptor.shape)
    store.clearTrace()
    val folded = zvalue(opened.foldFragments(selected, Vector.empty[ChunkFragment])(
      (fragments, fragment) => Right(FragmentControl.Continue(fragments :+ fragment))
    ))
    assertEquals(assemble(folded.state, selected.outputShape), Vector[Short](
      -9, -9, 19,
      5, 7, 9,
      -9, -9, 19
    ))
    assertEquals(folded.receipt.plannedChunks, 4)
    assertEquals(folded.receipt.visitedChunks, 4)
    assertEquals(folded.receipt.decodedChunks, 3)
    assertEquals(folded.receipt.fillChunks, 1)
    assertEquals(folded.receipt.emittedFragments, 4)
    assertEquals(folded.receipt.emittedElements, 9L)
    assertEquals(folded.receipt.requestedElements, 9L)
    assertEquals(folded.receipt.dataBytesRead, 36L)
    assert(folded.receipt.completed)
    assertEquals(folded.state.map(_.source).count(_ == FragmentSource.Fill), 1)
    assert(folded.state.forall(_.elementCount <= 4))
    assertEquals(store.trace.length, 4)

  test("fragment fold stops before the next store request"):
    val store = directStore
    val opened = zvalue(SyncZarr.openArray(store))
    val selected = selection(opened.descriptor.shape)
    store.clearTrace()
    val folded = zvalue(opened.foldFragments(selected, 0): (count, _) =>
      Right(FragmentControl.Stop(count + 1))
    )
    assertEquals(folded.state, 1)
    assertEquals(folded.receipt.visitedChunks, 1)
    assertEquals(folded.receipt.emittedFragments, 1)
    assert(!folded.receipt.completed)
    assertEquals(store.trace.length, 1)

  test("fragment fold propagates consumer and decode errors without further reads"):
    val store = directStore
    val opened = zvalue(SyncZarr.openArray(store))
    store.clearTrace()
    val consumerError = ZarrError.InvalidSelection("consumer refused fragment")
    val failed = opened.foldFragments(selection(opened.descriptor.shape), ()): (_, _) =>
      Left(consumerError)
    assertEquals(failed, Left(consumerError))
    assertEquals(store.trace.length, 1)

    val corrupt = zvalue(MemoryStore(Map(
      "zarr.json" -> bytes(directMetadata),
      "c/0/0" -> OwnedBytes.copyOf(Array[Byte](1, 2, 3))
    )))
    val corruptOpened = zvalue(SyncZarr.openArray(corrupt))
    var calls = 0
    val decoded = corruptOpened.foldFragments(selection(corruptOpened.descriptor.shape), ()): (_, _) =>
      calls += 1
      Right(FragmentControl.Continue(()))
    assert(decoded.isLeft)
    assertEquals(calls, 0)

  test("empty fragment selection completes without payload access"):
    val store = directStore
    val opened = zvalue(SyncZarr.openArray(store))
    val empty = zvalue(FactoredSelection.within(opened.descriptor.shape, Vector(
      AxisSelector.Slice(zvalue(AxisSlice(2L, 2L))),
      AxisSelector.All
    )))
    store.clearTrace()
    val folded = zvalue(opened.foldFragments(empty, 0): (count, _) =>
      Right(FragmentControl.Continue(count + 1))
    )
    assertEquals(folded.state, 0)
    assert(folded.receipt.completed)
    assertEquals(folded.receipt.requestedElements, 0L)
    assertEquals(store.trace, Vector.empty)

  test("fragment execution is runtime-rank for scalar and rank-five arrays"):
    val scalarMetadata =
      """{"zarr_format":3,"node_type":"array","shape":[],"data_type":"int16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}],"attributes":{},"storage_transformers":[]}"""
    val scalarStore = zvalue(MemoryStore(Map(
      "zarr.json" -> bytes(scalarMetadata),
      "c" -> int16(7)
    )))
    val scalar = zvalue(SyncZarr.openArray(scalarStore))
    val scalarFold = zvalue(scalar.foldFragments(
      FactoredSelection.all(scalar.descriptor.shape),
      Vector.empty[ChunkFragment]
    )((fragments, fragment) => Right(FragmentControl.Continue(fragments :+ fragment))))
    assertEquals(scalarFold.state.length, 1)
    assertEquals(scalarFold.state.head.shape.rank.toInt, 0)
    assertEquals(scalarFold.state.head.placement.rank.toInt, 0)
    scalarFold.state.head.values match
      case PrimitiveBlock.Int16(found) => assertEquals(found.toArray.toVector, Vector[Short](7))
      case _ => fail("expected scalar int16 fragment")

    val rankFiveMetadata =
      """{"zarr_format":3,"node_type":"array","shape":[1,1,1,1,2],"data_type":"int16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1,1,2]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}],"attributes":{},"storage_transformers":[]}"""
    val rankFiveStore = zvalue(MemoryStore(Map(
      "zarr.json" -> bytes(rankFiveMetadata),
      "c/0/0/0/0/0" -> int16(7, 8)
    )))
    val rankFive = zvalue(SyncZarr.openArray(rankFiveStore))
    val selected = zvalue(FactoredSelection.within(rankFive.descriptor.shape, Vector(
      AxisSelector.All,
      AxisSelector.All,
      AxisSelector.All,
      AxisSelector.All,
      AxisSelector.Indices(zvalue(AxisIndices.from(Vector(1L, 0L, 1L))))
    )))
    val rankFiveFold = zvalue(rankFive.foldFragments(
      selected,
      Vector.empty[ChunkFragment]
    )((fragments, fragment) => Right(FragmentControl.Continue(fragments :+ fragment))))
    assertEquals(rankFiveFold.state.head.shape.toVector, Vector(1L, 1L, 1L, 1L, 3L))
    rankFiveFold.state.head.values match
      case PrimitiveBlock.Int16(found) => assertEquals(found.toArray.toVector, Vector[Short](8, 7, 8))
      case _ => fail("expected rank-five int16 fragment")

  test("fragment receipts reject logical byte overflow through a typed boundary"):
    assert(FragmentMetrics.checked(1, Long.MaxValue, 8).isLeft)

  test("sharded fragment fold streams indexed payload and fill chunks"):
    val store = zvalue(MemoryStore(Map(
      "zarr.json" -> bytes(ZarrBinaryFixtures.shardedStartMetadata),
      "c/0/0" -> ZarrBinaryFixtures.shardedStartObject
    )))
    val opened = zvalue(SyncZarr.openArray(store))
    val selected = FactoredSelection.all(opened.descriptor.shape)
    store.clearTrace()
    val folded = zvalue(opened.foldFragments(selected, Vector.empty[ChunkFragment])(
      (fragments, fragment) => Right(FragmentControl.Continue(fragments :+ fragment))
    ))
    assertEquals(assemble(folded.state, selected.outputShape), Vector[Short](
      1, 2, 0, 0,
      3, 4, 0, 0,
      0, 0, 13, 14,
      0, 0, 15, 16
    ))
    assertEquals(folded.receipt.plannedChunks, 4)
    assertEquals(folded.receipt.decodedChunks, 2)
    assertEquals(folded.receipt.fillChunks, 2)
    assertEquals(folded.receipt.rangeRequests, 3)
    assertEquals(folded.receipt.indexBytesRead, 68L)
    assertEquals(folded.receipt.dataBytesRead, 16L)
    assert(folded.receipt.completed)
