package zarr4s.codec.blosc

import zarr4s.*

class BloscZstdMetadataSuite extends munit.FunSuite:
  test("capability compiles the normative Zstd profile"):
    val compiled = BloscZstdCapability.compile(
      extension(
        "cname" -> JsonValue.Str("zstd"),
        "clevel" -> number(5),
        "shuffle" -> JsonValue.Str("bitshuffle"),
        "typesize" -> number(4),
        "blocksize" -> number(0)
      ),
      BuiltInDataTypes.float32
    )

    compiled match
      case Right(codec: BloscZstdCodec) =>
        assertEquals(codec.compressionLevel.toInt, 5)
        assertEquals(codec.shuffle, BloscShuffle.BitShuffle)
        assertEquals(codec.typeSize.toInt, 4)
        assertEquals(codec.blockSize.toInt, 0)
      case Right(other) => fail(s"unexpected codec ${other.name}")
      case Left(error)  => fail(error)

  test("noshuffle may omit typesize and is canonicalized"):
    val compiled = BloscZstdCapability.compile(
      extension(
        "cname" -> JsonValue.Str("zstd"),
        "clevel" -> number(1),
        "shuffle" -> JsonValue.Str("noshuffle"),
        "blocksize" -> number(0)
      ),
      BuiltInDataTypes.uint8
    )

    compiled match
      case Right(codec: BloscZstdCodec) =>
        assertEquals(codec.typeSize.toInt, 1)
        assertEquals(codec.configuration.get("typesize"), Some(number(1)))
      case Right(other) => fail(s"unexpected codec ${other.name}")
      case Left(error)  => fail(error)

  test("profile rejects unsupported compressors and missing shuffle stride"):
    val wrongCompressor = BloscZstdCapability.compile(
      extension(
        "cname" -> JsonValue.Str("lz4"),
        "clevel" -> number(5),
        "shuffle" -> JsonValue.Str("shuffle"),
        "typesize" -> number(4),
        "blocksize" -> number(0)
      ),
      BuiltInDataTypes.float32
    )
    assert(wrongCompressor.left.exists(_.contains("requires cname 'zstd'")))

    val missingTypeSize = BloscZstdCapability.compile(
      extension(
        "cname" -> JsonValue.Str("zstd"),
        "clevel" -> number(5),
        "shuffle" -> JsonValue.Str("shuffle"),
        "blocksize" -> number(0)
      ),
      BuiltInDataTypes.float32
    )
    assert(missingTypeSize.left.exists(_.contains("typesize is required")))

  test("provider adds optional codecs without changing the core"):
    assertEquals(ZarrCapabilities().codec("blosc"), None)
    assertEquals(ZarrCapabilities().codec("zstd"), None)
    val extended = BloscZstdProvider.capabilities()
    assertEquals(extended.codec("blosc"), Some(BloscZstdCapability))
    assertEquals(extended.codec("zstd"), Some(ZstdCapability))
    assertEquals(extended.codecs.count(_.name == "blosc"), 1)
    assertEquals(extended.codecs.count(_.name == "zstd"), 1)

  private def extension(fields: (String, JsonValue)*): ExtensionMetadata =
    ExtensionMetadata(
      "blosc",
      JsonObject.unsafe(fields.toVector),
      mustUnderstand = true,
      JsonObject.empty
    )

  private def number(value: Int): JsonValue =
    JsonValue.Num(JsonNumber.unsafe(value.toString))
