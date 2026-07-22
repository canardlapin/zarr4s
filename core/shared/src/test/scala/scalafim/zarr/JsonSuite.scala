package scalafim.zarr

class JsonSuite extends munit.FunSuite:
  private def value[A](result: Either[JsonError, A]): A = result match
    case Right(found) => found
    case Left(error) => fail(error.message)

  test("parser preserves exact number lexemes"):
    val parsed = value(JsonParser.parse("[9007199254740993,-0,1.25e-30]"))
    parsed match
      case JsonValue.Arr(values) =>
        assertEquals(values.collect { case JsonValue.Num(number) => number.lexeme },
          Vector("9007199254740993", "-0", "1.25e-30"))
      case _ => fail("expected array")

  test("parser rejects duplicate keys and malformed numbers"):
    assert(JsonParser.parse("{\"shape\":[],\"shape\":[1]}").isLeft)
    Seq("01", "1.", "1e", "--1", "+1").foreach: input =>
      assert(JsonParser.parse(input).isLeft, input)

  test("parser validates raw and escaped surrogate pairs"):
    val musical = "\ud834\udd1e"
    assertEquals(value(JsonParser.parse(s"\"$musical\"")), JsonValue.Str(musical))
    assertEquals(value(JsonParser.parse("\"\\uD834\\uDD1E\"")), JsonValue.Str(musical))
    assert(JsonParser.parse("\"\\uD834x\"").isLeft)
    assert(JsonParser.parse("\"\\uDD1E\"").isLeft)

  test("renderer is deterministic and orders object keys"):
    val parsed = value(JsonParser.parse("{\"z\":1,\"a\":\"line\\n\",\"m\":true}"))
    assertEquals(parsed.render, "{\"a\":\"line\\n\",\"m\":true,\"z\":1}")
    assertEquals(value(JsonParser.parse(parsed.render)), parsed match
      case JsonValue.Obj(obj) => JsonValue.Obj(JsonObject.unsafe(obj.fields.sortBy(_._1)))
      case other => other
    )

  test("resource limits fail explicitly"):
    assert(JsonParser.parse("[1,2]", JsonLimits(maxArrayLength = 1)).isLeft)
    assert(JsonParser.parse("[[0]]", JsonLimits(maxDepth = 1)).isLeft)
    assert(JsonParser.parse("\"abcd\"", JsonLimits(maxStringLength = 3)).isLeft)
