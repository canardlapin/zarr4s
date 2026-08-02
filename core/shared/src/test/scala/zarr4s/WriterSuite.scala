package zarr4s

class WriterSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def complete(outcome: WriteOutcome): WriteReceipt = outcome match
    case WriteOutcome.Complete(receipt)    => receipt
    case WriteOutcome.Incomplete(_, error) => fail(error.message)

  private def descriptor(metadata: String): ArrayDescriptor =
    ZarrMetadata.parse(metadata) match
      case Right(ZarrNodeMetadata.Array(array)) => zvalue(ArrayDescriptor.compile(array))
      case Right(_)                             => fail("expected array metadata")
      case Left(error)                          => fail(error.message)

  private def constant(block: PrimitiveBlock): ChunkProvider = new ChunkProvider:
    def chunk(
        coordinate: ChunkCoordinate,
        storedShape: Shape
    ): Either[ZarrError, ChunkPayload] = Right(ChunkPayload.Values(block))

  private def readAll(
      store: ObjectReader,
      path: ZarrPath,
      found: ArrayDescriptor
  ): PrimitiveBlock =
    val opened = zvalue(SyncZarr.openArray(store, path))
    val origin = zvalue(Coordinate.from(Vector.fill(found.shape.rank.toInt)(0L)))
    val region = zvalue(Region.within(found.shape, origin, found.shape))
    zvalue(opened.readRegion(region)).block

  private def assertBlockEquals(actual: PrimitiveBlock, expected: PrimitiveBlock): Unit =
    (actual, expected) match
      case (PrimitiveBlock.Bool(left), PrimitiveBlock.Bool(right)) =>
        assertEquals(left.toArray.toVector, right.toArray.toVector)
      case (PrimitiveBlock.Int8(left), PrimitiveBlock.Int8(right)) =>
        assertEquals(left.toArray.toVector, right.toArray.toVector)
      case (PrimitiveBlock.UInt8(left), PrimitiveBlock.UInt8(right)) =>
        assertEquals(left.toArray.toVector, right.toArray.toVector)
      case (PrimitiveBlock.Int16(left), PrimitiveBlock.Int16(right)) =>
        assertEquals(left.toArray.toVector, right.toArray.toVector)
      case (PrimitiveBlock.UInt16(left), PrimitiveBlock.UInt16(right)) =>
        assertEquals(left.toArray.toVector, right.toArray.toVector)
      case (PrimitiveBlock.Int32(left), PrimitiveBlock.Int32(right)) =>
        assertEquals(left.toArray.toVector, right.toArray.toVector)
      case (PrimitiveBlock.UInt32(left), PrimitiveBlock.UInt32(right)) =>
        assertEquals(left.toArray.toVector, right.toArray.toVector)
      case (PrimitiveBlock.Int64(left), PrimitiveBlock.Int64(right)) =>
        assertEquals(left.toArray.toVector, right.toArray.toVector)
      case (PrimitiveBlock.UInt64(left), PrimitiveBlock.UInt64(right)) =>
        assertEquals(left.toArray.toVector, right.toArray.toVector)
      case (PrimitiveBlock.Float32(left), PrimitiveBlock.Float32(right)) =>
        assertEquals(left.toArray.toVector, right.toArray.toVector)
      case (PrimitiveBlock.Float64(left), PrimitiveBlock.Float64(right)) =>
        assertEquals(left.toArray.toVector, right.toArray.toVector)
      case (PrimitiveBlock.Float16(left), PrimitiveBlock.Float16(right)) =>
        assertEquals(left.toArray.toVector, right.toArray.toVector)
      case (PrimitiveBlock.Complex64(left), PrimitiveBlock.Complex64(right)) =>
        assertEquals(left.toInterleavedArray.toVector, right.toInterleavedArray.toVector)
      case (PrimitiveBlock.Complex128(left), PrimitiveBlock.Complex128(right)) =>
        assertEquals(left.toInterleavedArray.toVector, right.toInterleavedArray.toVector)
      case (PrimitiveBlock.Raw(left, leftWidth), PrimitiveBlock.Raw(right, rightWidth)) =>
        assertEquals(leftWidth, rightWidth)
        assertEquals(left.toArray.toVector, right.toArray.toVector)
      case _ => fail(s"block type mismatch: $actual versus $expected")

  test("portable SHA-256 and group creation are deterministic"):
    assertEquals(
      PortableSha256.digestUtf8("abc").value,
      "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    )
    val paddingOracles = Vector(
      0 -> "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
      55 -> "9f4390f8d30c2dd92ec9f095b65e2b9ae9b0a925a5258e241c9f1e910f734318",
      56 -> "b35439a4ac6f0948b6d6f9e3c6af0f5f590ce20f1bde7090ef7970686ec6738a",
      63 -> "7d3e74a05d7db15bce4ad9ec0658ea98e3f06eeecf16b4c6fff2da457ddc2f34",
      64 -> "ffe054fe7ae0cb6dc65c3af9b61d5209f439851db43d0ba5997337df154668eb",
      65 -> "635361c48bb9eab14198e76ea8ab7f1a41685d6ad62aa9146d301d4f17eb0ae0"
    )
    paddingOracles.foreach: (length, expected) =>
      assertEquals(
        PortableSha256.digest(Array.fill[Byte](length)('a'.toByte)).value,
        expected
      )
    val store = zvalue(MemoryStore(Map.empty))
    val path = zvalue(ZarrPath("study"))
    val metadata = GroupMetadata(
      JsonObject.unsafe(Vector("title" -> JsonValue.Str("portable"))),
      JsonObject.empty
    )
    val receipt = complete(SyncZarrWriter.createGroup(store, metadata, path))
    assertEquals(receipt.metadata.key.value, "study/zarr.json")
    assertEquals(receipt.totalObjects, 1)
    assertEquals(receipt.progress.visitedChunks, 0L)
    assertEquals(
      store.writeTrace,
      Vector(ObjectWrite.Create(receipt.metadata.key, receipt.metadata.length))
    )
    val opened = zvalue(SyncZarr.openGroup(store, path))
    assertEquals(opened.metadata.attributes, metadata.attributes)

  test("all fixed-width scalar carriers round trip through a prefixed v2-key writer"):
    val carriers = Vector[(String, String, PrimitiveBlock)](
      ("bool", "false", PrimitiveBlock.Bool(OwnedBooleans.copyOf(Array(true, false, true, true)))),
      ("int8", "0", PrimitiveBlock.Int8(OwnedBytes.copyOf(Array[Byte](-3, 0, 4, 127)))),
      ("int16", "0", PrimitiveBlock.Int16(OwnedShorts.copyOf(Array[Short](-300, 0, 4, 300)))),
      ("int32", "0", PrimitiveBlock.Int32(OwnedInts.copyOf(Array(-70000, 0, 4, 70000)))),
      ("int64", "0", PrimitiveBlock.Int64(OwnedLongs.copyOf(Array(-9L, 0L, 4L, Long.MaxValue)))),
      ("uint8", "0", PrimitiveBlock.UInt8(OwnedBytes.copyOf(Array[Byte](0, 1, -2, -1)))),
      ("uint16", "0", PrimitiveBlock.UInt16(OwnedShorts.copyOf(Array[Short](0, 1, -2, -1)))),
      ("uint32", "0", PrimitiveBlock.UInt32(OwnedInts.copyOf(Array(0, 1, -2, -1)))),
      ("uint64", "0", PrimitiveBlock.UInt64(OwnedLongs.copyOf(Array(0L, 1L, -2L, -1L)))),
      (
        "float32",
        "0.0",
        PrimitiveBlock.Float32(OwnedFloats.copyOf(Array(-1.5f, 0.0f, 2.25f, 9.0f)))
      ),
      ("float64", "0.0", PrimitiveBlock.Float64(OwnedDoubles.copyOf(Array(-1.5, 0.0, 2.25, 9.0)))),
      (
        "float16",
        "\"0x0000\"",
        PrimitiveBlock.Float16(OwnedShorts.copyOf(Array[Short](0x3c00, 0, 0x4100, 0x4800)))
      ),
      (
        "complex64",
        "[0.0,0.0]",
        PrimitiveBlock.Complex64(
          OwnedComplex64.copyOfInterleaved(Array(1.5f, -2.0f, 0.0f, 0.5f, 2.25f, 1.0f, 9.0f, -3.0f))
        )
      ),
      (
        "complex128",
        "[0.0,0.0]",
        PrimitiveBlock.Complex128(
          OwnedComplex128.copyOfInterleaved(Array(1.5, -2.0, 0.0, 0.5, 2.25, 1.0, 9.0, -3.0))
        )
      ),
      (
        "r24",
        "[0,0,0]",
        PrimitiveBlock.Raw(
          OwnedBytes.copyOf(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)),
          3
        )
      )
    )
    val store = zvalue(MemoryStore(Map.empty))
    carriers.foreach: (name, fill, block) =>
      val endian =
        if Set("bool", "int8", "uint8").contains(name) || name.startsWith("r") then ""
        else "\"configuration\":{\"endian\":\"big\"},"
      val found = descriptor(
        s"""{"zarr_format":3,"node_type":"array","shape":[2,2],"data_type":"$name","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,2]}},"chunk_key_encoding":{"name":"v2"},"fill_value":$fill,"codecs":[{${endian}"name":"bytes"}],"attributes":{},"storage_transformers":[]}"""
      )
      val path = zvalue(ZarrPath(s"scalars/$name"))
      val receipt = complete(SyncZarrWriter.create(store, found, constant(block), path))
      assertEquals(receipt.visitedChunks, 1L)
      assertEquals(receipt.encodedChunks, 1L)
      assertEquals(receipt.objects.map(_.key.value), Vector(s"scalars/$name/0.0"))
      assertBlockEquals(readAll(store, path, found), block)

  test("transpose and arbitrary rank preserve logical element order"):
    val cases = Vector(
      (
        "scalar",
        "[]",
        "[]",
        "int16",
        "0",
        "{\"configuration\":{\"endian\":\"little\"},\"name\":\"bytes\"}",
        PrimitiveBlock.Int16(OwnedShorts.copyOf(Array[Short](7)))
      ),
      (
        "rank5",
        "[1,1,1,2,3]",
        "[1,1,1,2,3]",
        "int32",
        "0",
        "{\"configuration\":{\"order\":[4,3,2,1,0]},\"name\":\"transpose\"},{\"configuration\":{\"endian\":\"little\"},\"name\":\"bytes\"}",
        PrimitiveBlock.Int32(OwnedInts.copyOf(Array.range(0, 6)))
      )
    )
    cases.foreach: (_, shape, chunkShape, dataType, fill, codecs, block) =>
      val found = descriptor(
        s"""{"zarr_format":3,"node_type":"array","shape":$shape,"data_type":"$dataType","chunk_grid":{"name":"regular","configuration":{"chunk_shape":$chunkShape}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":$fill,"codecs":[$codecs],"attributes":{},"storage_transformers":[]}"""
      )
      val store = zvalue(MemoryStore(Map.empty))
      val receipt = complete(SyncZarrWriter.create(store, found, constant(block)))
      assertEquals(receipt.visitedChunks, 1L)
      assertBlockEquals(readAll(store, ZarrPath.root, found), block)

    var calls = 0
    val empty = descriptor(
      """{"zarr_format":3,"node_type":"array","shape":[0,5],"data_type":"int16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,3]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}],"attributes":{},"storage_transformers":[]}"""
    )
    val provider = new ChunkProvider:
      def chunk(
          coordinate: ChunkCoordinate,
          storedShape: Shape
      ): Either[ZarrError, ChunkPayload] =
        calls += 1
        Left(ZarrError.WriteFailure("empty arrays must not request a chunk"))
    val emptyStore = zvalue(MemoryStore(Map.empty))
    val emptyReceipt = complete(SyncZarrWriter.create(emptyStore, empty, provider))
    assertEquals(calls, 0)
    assertEquals(emptyReceipt.objects, Vector.empty)
    assertEquals(emptyReceipt.totalObjects, 1)

  test("start and end indexed shards reproduce Zarr-Python objects"):
    val cases = Vector(
      ZarrBinaryFixtures.shardedStartMetadata -> ZarrBinaryFixtures.shardedStartObject,
      ZarrBinaryFixtures.shardedEndMetadata -> ZarrBinaryFixtures.shardedEndObject
    )
    cases.foreach: (metadata, expectedObject) =>
      val found = descriptor(metadata)
      val provider = new ChunkProvider:
        def chunk(
            coordinate: ChunkCoordinate,
            storedShape: Shape
        ): Either[ZarrError, ChunkPayload] = coordinate.toVector match
          case Vector(0L, 0L) =>
            Right(
              ChunkPayload.Values(
                PrimitiveBlock.Int16(OwnedShorts.copyOf(Array[Short](1, 2, 3, 4)))
              )
            )
          case Vector(1L, 1L) =>
            Right(
              ChunkPayload.Values(
                PrimitiveBlock.Int16(OwnedShorts.copyOf(Array[Short](13, 14, 15, 16)))
              )
            )
          case _ => Right(ChunkPayload.Fill)
      val store = zvalue(MemoryStore(Map.empty))
      val receipt = complete(SyncZarrWriter.create(store, found, provider))
      assertEquals(store.snapshot("c/0/0"), expectedObject)
      assertEquals(receipt.visitedChunks, 4L)
      assertEquals(receipt.encodedChunks, 2L)
      assertEquals(receipt.omittedFillChunks, 2L)
      assertEquals(receipt.paddingChunks, 0L)
      assertEquals(readAll(store, ZarrPath.root, found).elementCount, 16)

  test("conflicts and limits expose partial progress without a completion marker"):
    val found = descriptor(
      """{"zarr_format":3,"node_type":"array","shape":[2,4],"data_type":"int16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,2]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}],"attributes":{},"storage_transformers":[]}"""
    )
    val provider = constant(
      PrimitiveBlock.Int16(
        OwnedShorts.copyOf(Array[Short](1, 2, 3, 4))
      )
    )
    val conflict = zvalue(
      MemoryStore(
        Map(
          "c/0/1" -> OwnedBytes.copyOf(Array[Byte](99))
        )
      )
    )
    SyncZarrWriter.create(conflict, found, provider) match
      case WriteOutcome.Complete(_) => fail("conflicting creation must be incomplete")
      case WriteOutcome.Incomplete(progress, error) =>
        assertEquals(
          error,
          ZarrError.StoreFailure(
            StoreError.AlreadyExists(zvalue(StoreKey.from("c/0/1")))
          )
        )
        assertEquals(progress.objects.map(_.key.value), Vector("c/0/0"))
        assertEquals(progress.visitedChunks, 2L)
        assertEquals(progress.encodedChunks, 2L)
        assert(!conflict.snapshot.contains("zarr.json"))

    val limited = zvalue(MemoryStore(Map.empty))
    val limits = WriterLimits(maxObjects = 1)
    SyncZarrWriter.create(limited, found, provider, limits = limits) match
      case WriteOutcome.Complete(_) => fail("object limit must refuse data creation")
      case WriteOutcome.Incomplete(progress, error) =>
        assertEquals(error, ZarrError.ResourceLimit("written objects", 1L, 2L))
        assertEquals(progress.objects, Vector.empty)
        assertEquals(limited.writeTrace, Vector.empty)
