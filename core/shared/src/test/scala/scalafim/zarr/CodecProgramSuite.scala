package scalafim.zarr

class CodecProgramSuite extends munit.FunSuite:
  private final case class TestBytesCodec(name: String) extends CompiledCodec:
    val input = CodecRepresentation.Bytes
    val output = CodecRepresentation.Bytes
    val configuration = JsonObject.empty

  private final case class TestArrayCodec(name: String) extends CompiledCodec:
    val input = CodecRepresentation.ArrayValues
    val output = CodecRepresentation.ArrayValues
    val configuration = JsonObject.empty

  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error) => fail(error.message)

  test("array programs contain one lawful transition to bytes"):
    val program = zvalue(CodecProgram.compile(
      CodecRepresentation.ArrayValues,
      Vector(
        TestArrayCodec("transpose"),
        BytesCodec(Some(Endianness.Little)),
        TestBytesCodec("compress"),
        Crc32cCodec
      )
    ))
    assertEquals(program.initial, CodecRepresentation.ArrayValues)
    assertEquals(program.output, CodecRepresentation.Bytes)
    assertEquals(program.executorRequirements, Vector("compress", "crc32c"))

  test("invalid representation transitions cannot form a program"):
    assert(CodecProgram.compile(
      CodecRepresentation.ArrayValues,
      Vector(TestBytesCodec("compress"))
    ).isLeft)
    assert(CodecProgram.compile(
      CodecRepresentation.ArrayValues,
      Vector(BytesCodec(Some(Endianness.Little)), TestArrayCodec("late-array"))
    ).isLeft)
    assert(CodecProgram.compile(
      CodecRepresentation.ArrayValues,
      Vector(TestArrayCodec("identity"))
    ).isLeft)

  test("bytes identity and bytes-to-bytes programs are lawful"):
    assert(CodecProgram.bytesIdentity.isEmpty)
    assertEquals(CodecProgram.bytesIdentity.initial, CodecRepresentation.Bytes)
    assertEquals(CodecProgram.bytesIdentity.output, CodecRepresentation.Bytes)

    val program = zvalue(CodecProgram.compile(
      CodecRepresentation.Bytes,
      Vector(TestBytesCodec("filter"), TestBytesCodec("filter"))
    ))
    assertEquals(program.executorRequirements, Vector("filter"))

  test("shard index program admits only little-endian uint64 bytes plus CRC32C"):
    val valid = ShardIndexProgram.compile(Vector(
      BytesCodec(Some(Endianness.Little)),
      Crc32cCodec
    ))
    assertEquals(
      valid.map(_.codecs.stages.map(_.name)),
      Right(Vector("bytes", "crc32c"))
    )
    assert(ShardIndexProgram.compile(Vector(
      BytesCodec(Some(Endianness.Big)),
      Crc32cCodec
    )).isLeft)
    assert(ShardIndexProgram.compile(Vector(
      BytesCodec(Some(Endianness.Little))
    )).isLeft)
