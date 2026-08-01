package zarr4s.codec.blosc

import zarr4s.*

class ZstdMetadataSuite extends munit.FunSuite:
  test("capability compiles level and checksum configuration"):
    val compiled = ZstdCapability.compile(
      extension(
        "level" -> JsonValue.Num(JsonNumber.unsafe("5")),
        "checksum" -> JsonValue.Bool(true)
      ),
      BuiltInDataTypes.int16
    )

    compiled match
      case Right(codec: ZstdCodec) =>
        assertEquals(codec.compressionLevel.toInt, 5)
        assert(codec.checksum)
        assertEquals(codec.configuration.get("level"), Some(JsonValue.Num(JsonNumber.unsafe("5"))))
      case Right(other) => fail(s"unexpected codec ${other.name}")
      case Left(error)  => fail(error)

  test("missing optional fields use numcodecs-compatible defaults"):
    val compiled = ZstdCapability.compile(extension(), BuiltInDataTypes.uint8)
    compiled match
      case Right(codec: ZstdCodec) =>
        assertEquals(codec.compressionLevel.toInt, 0)
        assert(!codec.checksum)
      case Right(other) => fail(s"unexpected codec ${other.name}")
      case Left(error)  => fail(error)

  test("provider adds zstd without changing the core"):
    assertEquals(ZarrCapabilities().codec("zstd"), None)
    val extended = ZstdProvider.capabilities()
    assertEquals(extended.codec("zstd"), Some(ZstdCapability))
    assertEquals(extended.codecs.count(_.name == "zstd"), 1)

  private def extension(fields: (String, JsonValue)*): ExtensionMetadata =
    ExtensionMetadata(
      "zstd",
      JsonObject.unsafe(fields.toVector),
      mustUnderstand = true,
      JsonObject.empty
    )
