package zarr4s

enum JsonError:
  case UnexpectedEnd(offset: Int, expected: String)
  case UnexpectedCharacter(offset: Int, found: Char, expected: String)
  case InvalidNumber(offset: Int, detail: String)
  case InvalidEscape(offset: Int, detail: String)
  case InvalidUnicode(offset: Int, detail: String)
  case DuplicateKey(offset: Int, key: String)
  case TrailingInput(offset: Int)
  case LimitExceeded(offset: Int, resource: String, limit: Int)

  def message: String = this match
    case UnexpectedEnd(offset, expected) => s"unexpected end at $offset; expected $expected"
    case UnexpectedCharacter(offset, found, expected) =>
      s"unexpected character '$found' at $offset; expected $expected"
    case InvalidNumber(offset, detail)          => s"invalid number at $offset: $detail"
    case InvalidEscape(offset, detail)          => s"invalid escape at $offset: $detail"
    case InvalidUnicode(offset, detail)         => s"invalid Unicode at $offset: $detail"
    case DuplicateKey(offset, key)              => s"duplicate object key '$key' at $offset"
    case TrailingInput(offset)                  => s"trailing input at $offset"
    case LimitExceeded(offset, resource, limit) =>
      s"$resource limit $limit exceeded at $offset"

final class JsonNumber private (val lexeme: String):
  def isInteger: Boolean =
    !lexeme.exists(character => character == '.' || character == 'e' || character == 'E')

  def toLongExact: Either[String, Long] =
    if !isInteger then Left(s"expected an integer, found $lexeme")
    else
      try Right(java.lang.Long.parseLong(lexeme))
      catch
        case _: NumberFormatException =>
          Left(s"integer is outside the signed 64-bit range: $lexeme")

  def toBigIntExact: Either[String, BigInt] =
    if !isInteger then Left(s"expected an integer, found $lexeme")
    else
      try Right(BigInt(lexeme))
      catch case _: NumberFormatException => Left(s"invalid integer: $lexeme")

  def toDouble: Double = java.lang.Double.parseDouble(lexeme)

  override def equals(other: Any): Boolean = other match
    case that: JsonNumber => lexeme == that.lexeme
    case _                => false

  override def hashCode(): Int = lexeme.hashCode

  override def toString: String = lexeme

object JsonNumber:
  private[zarr4s] def unsafe(lexeme: String): JsonNumber = new JsonNumber(lexeme)

enum JsonValue:
  case Null
  case Bool(value: Boolean)
  case Num(value: JsonNumber)
  case Str(value: String)
  case Arr(values: Vector[JsonValue])
  case Obj(value: JsonObject)

  def render: String = JsonRenderer.render(this)

final class JsonObject private (val fields: Vector[(String, JsonValue)]):
  private val index: Map[String, JsonValue] = fields.toMap

  def get(name: String): Option[JsonValue] = index.get(name)

  def contains(name: String): Boolean = index.contains(name)

  def names: Vector[String] = fields.map(_._1)

  def removed(names: Set[String]): JsonObject =
    JsonObject.unsafe(fields.filterNot((name, _) => names.contains(name)))

  override def equals(other: Any): Boolean = other match
    case that: JsonObject => fields == that.fields
    case _                => false

  override def hashCode(): Int = fields.hashCode

  override def toString: String = JsonValue.Obj(this).render

object JsonObject:
  val empty: JsonObject = new JsonObject(Vector.empty)

  def from(fields: Seq[(String, JsonValue)]): Either[String, JsonObject] =
    val seen = scala.collection.mutable.HashSet.empty[String]
    val copied = Vector.newBuilder[(String, JsonValue)]
    val iterator = fields.iterator
    while iterator.hasNext do
      val field = iterator.next()
      if seen.contains(field._1) then return Left(s"duplicate object key '${field._1}'")
      seen += field._1
      copied += field
    Right(new JsonObject(copied.result()))

  private[zarr4s] def unsafe(fields: Vector[(String, JsonValue)]): JsonObject =
    new JsonObject(fields)

final case class JsonLimits(
    maxInputLength: Int = 16 * 1024 * 1024,
    maxDepth: Int = 128,
    maxStringLength: Int = 4 * 1024 * 1024,
    maxArrayLength: Int = 1000000,
    maxObjectFields: Int = 100000
):
  require(maxInputLength >= 0, "maxInputLength must be non-negative")
  require(maxDepth >= 0, "maxDepth must be non-negative")
  require(maxStringLength >= 0, "maxStringLength must be non-negative")
  require(maxArrayLength >= 0, "maxArrayLength must be non-negative")
  require(maxObjectFields >= 0, "maxObjectFields must be non-negative")

object JsonParser:
  def parse(input: String, limits: JsonLimits = JsonLimits()): Either[JsonError, JsonValue] =
    if input.length > limits.maxInputLength then
      Left(JsonError.LimitExceeded(0, "input length", limits.maxInputLength))
    else new Parser(input, limits).parse()

  private final class Parser(input: String, limits: JsonLimits):
    private var offset = 0

    def parse(): Either[JsonError, JsonValue] =
      whitespace()
      value(0).flatMap: result =>
        whitespace()
        if offset == input.length then Right(result)
        else Left(JsonError.TrailingInput(offset))

    private def value(depth: Int): Either[JsonError, JsonValue] =
      if depth > limits.maxDepth then
        Left(JsonError.LimitExceeded(offset, "nesting depth", limits.maxDepth))
      else if offset >= input.length then Left(JsonError.UnexpectedEnd(offset, "a JSON value"))
      else
        input.charAt(offset) match
          case 'n'                     => literal("null", JsonValue.Null)
          case 't'                     => literal("true", JsonValue.Bool(true))
          case 'f'                     => literal("false", JsonValue.Bool(false))
          case '"'                     => string().map(JsonValue.Str.apply)
          case '['                     => array(depth + 1)
          case '{'                     => obj(depth + 1)
          case '-'                     => number().map(JsonValue.Num.apply)
          case digit if isDigit(digit) => number().map(JsonValue.Num.apply)
          case found => Left(JsonError.UnexpectedCharacter(offset, found, "a JSON value"))

    private def literal(expected: String, result: JsonValue): Either[JsonError, JsonValue] =
      if input.regionMatches(offset, expected, 0, expected.length) then
        offset += expected.length
        Right(result)
      else Left(JsonError.UnexpectedCharacter(offset, input.charAt(offset), expected))

    private def number(): Either[JsonError, JsonNumber] =
      val start = offset
      if current('-') then offset += 1
      if offset >= input.length then
        return Left(JsonError.InvalidNumber(start, "missing integer digits"))

      if current('0') then
        offset += 1
        if offset < input.length && isDigit(input.charAt(offset)) then
          return Left(JsonError.InvalidNumber(start, "leading zero"))
      else if isNonZeroDigit(input.charAt(offset)) then
        while offset < input.length && isDigit(input.charAt(offset)) do offset += 1
      else return Left(JsonError.InvalidNumber(start, "missing integer digits"))

      if current('.') then
        offset += 1
        val fractionStart = offset
        while offset < input.length && isDigit(input.charAt(offset)) do offset += 1
        if offset == fractionStart then
          return Left(JsonError.InvalidNumber(start, "missing fractional digits"))

      if current('e') || current('E') then
        offset += 1
        if current('+') || current('-') then offset += 1
        val exponentStart = offset
        while offset < input.length && isDigit(input.charAt(offset)) do offset += 1
        if offset == exponentStart then
          return Left(JsonError.InvalidNumber(start, "missing exponent digits"))

      Right(JsonNumber.unsafe(input.substring(start, offset)))

    private def string(): Either[JsonError, String] =
      val start = offset
      offset += 1
      val result = new java.lang.StringBuilder
      while offset < input.length do
        val character = input.charAt(offset)
        if character == '"' then
          offset += 1
          return Right(result.toString)
        else if character == '\\' then
          offset += 1
          escape(result) match
            case Left(error) => return Left(error)
            case Right(_)    => ()
        else if character < ' ' then
          return Left(
            JsonError.UnexpectedCharacter(offset, character, "an escaped control character")
          )
        else if Character.isHighSurrogate(character) then
          if offset + 1 >= input.length || !Character.isLowSurrogate(input.charAt(offset + 1)) then
            return Left(JsonError.InvalidUnicode(offset, "unpaired high surrogate"))
          result.append(character)
          result.append(input.charAt(offset + 1))
          offset += 2
        else if Character.isLowSurrogate(character) then
          return Left(JsonError.InvalidUnicode(offset, "unpaired low surrogate"))
        else
          result.append(character)
          offset += 1

        if result.length > limits.maxStringLength then
          return Left(JsonError.LimitExceeded(start, "string length", limits.maxStringLength))
      Left(JsonError.UnexpectedEnd(offset, "closing quote"))

    private def escape(result: java.lang.StringBuilder): Either[JsonError, Unit] =
      if offset >= input.length then Left(JsonError.UnexpectedEnd(offset, "escape character"))
      else
        input.charAt(offset) match
          case '"'   => result.append('"'); offset += 1; Right(())
          case '\\'  => result.append('\\'); offset += 1; Right(())
          case '/'   => result.append('/'); offset += 1; Right(())
          case 'b'   => result.append('\b'); offset += 1; Right(())
          case 'f'   => result.append('\f'); offset += 1; Right(())
          case 'n'   => result.append('\n'); offset += 1; Right(())
          case 'r'   => result.append('\r'); offset += 1; Right(())
          case 't'   => result.append('\t'); offset += 1; Right(())
          case 'u'   => unicodeEscape(result)
          case found => Left(JsonError.InvalidEscape(offset, s"unsupported escape '$found'"))

    private def unicodeEscape(result: java.lang.StringBuilder): Either[JsonError, Unit] =
      val escapeOffset = offset - 1
      offset += 1
      hexCodeUnit() match
        case Left(error)                                    => Left(error)
        case Right(high) if Character.isHighSurrogate(high) =>
          if offset + 1 >= input.length || input.charAt(offset) != '\\' || input.charAt(
              offset + 1
            ) != 'u'
          then
            Left(
              JsonError.InvalidUnicode(escapeOffset, "escaped high surrogate lacks a low surrogate")
            )
          else
            offset += 2
            hexCodeUnit().flatMap: low =>
              if !Character.isLowSurrogate(low) then
                Left(JsonError.InvalidUnicode(offset - 4, "expected an escaped low surrogate"))
              else
                result.append(high)
                result.append(low)
                Right(())
        case Right(low) if Character.isLowSurrogate(low) =>
          Left(JsonError.InvalidUnicode(escapeOffset, "unpaired escaped low surrogate"))
        case Right(character) =>
          result.append(character)
          Right(())

    private def hexCodeUnit(): Either[JsonError, Char] =
      if offset + 4 > input.length then
        Left(JsonError.UnexpectedEnd(offset, "four hexadecimal digits"))
      else
        var value = 0
        var index = 0
        while index < 4 do
          val digit = Character.digit(input.charAt(offset + index), 16)
          if digit < 0 then
            return Left(JsonError.InvalidUnicode(offset + index, "expected a hexadecimal digit"))
          value = value * 16 + digit
          index += 1
        offset += 4
        Right(value.toChar)

    private def array(depth: Int): Either[JsonError, JsonValue] =
      offset += 1
      whitespace()
      val values = Vector.newBuilder[JsonValue]
      var count = 0
      if current(']') then
        offset += 1
        Right(JsonValue.Arr(Vector.empty))
      else
        while true do
          if count >= limits.maxArrayLength then
            return Left(JsonError.LimitExceeded(offset, "array length", limits.maxArrayLength))
          value(depth) match
            case Left(error)  => return Left(error)
            case Right(found) => values += found
          count += 1
          whitespace()
          if current(']') then
            offset += 1
            return Right(JsonValue.Arr(values.result()))
          else if current(',') then
            offset += 1
            whitespace()
          else return expected("',' or ']' in array")
        Left(JsonError.UnexpectedEnd(offset, "array terminator"))

    private def obj(depth: Int): Either[JsonError, JsonValue] =
      offset += 1
      whitespace()
      val fields = Vector.newBuilder[(String, JsonValue)]
      val seen = scala.collection.mutable.HashSet.empty[String]
      var count = 0
      if current('}') then
        offset += 1
        Right(JsonValue.Obj(JsonObject.empty))
      else
        while true do
          if count >= limits.maxObjectFields then
            return Left(
              JsonError.LimitExceeded(offset, "object field count", limits.maxObjectFields)
            )
          if !current('"') then return expected("an object key")
          val keyOffset = offset
          val key = string() match
            case Left(error)  => return Left(error)
            case Right(found) => found
          if seen.contains(key) then return Left(JsonError.DuplicateKey(keyOffset, key))
          seen += key
          whitespace()
          if !current(':') then return expected("':' after object key")
          offset += 1
          whitespace()
          value(depth) match
            case Left(error)  => return Left(error)
            case Right(found) => fields += ((key, found))
          count += 1
          whitespace()
          if current('}') then
            offset += 1
            return Right(JsonValue.Obj(JsonObject.unsafe(fields.result())))
          else if current(',') then
            offset += 1
            whitespace()
          else return expected("',' or '}' in object")
        Left(JsonError.UnexpectedEnd(offset, "object terminator"))

    private def whitespace(): Unit =
      while offset < input.length && isWhitespace(input.charAt(offset)) do offset += 1

    private def current(character: Char): Boolean =
      offset < input.length && input.charAt(offset) == character

    private def expected[A](description: String): Left[JsonError, A] =
      if offset >= input.length then Left(JsonError.UnexpectedEnd(offset, description))
      else Left(JsonError.UnexpectedCharacter(offset, input.charAt(offset), description))

    private inline def isDigit(character: Char): Boolean = character >= '0' && character <= '9'

    private inline def isNonZeroDigit(character: Char): Boolean =
      character >= '1' && character <= '9'

    private inline def isWhitespace(character: Char): Boolean =
      character == ' ' || character == '\n' || character == '\r' || character == '\t'

object JsonRenderer:
  def render(value: JsonValue): String =
    val result = new java.lang.StringBuilder
    append(value, result)
    result.toString

  private def append(value: JsonValue, result: java.lang.StringBuilder): Unit = value match
    case JsonValue.Null        => appendRaw("null", result)
    case JsonValue.Bool(found) => appendRaw(if found then "true" else "false", result)
    case JsonValue.Num(found)  => appendRaw(found.lexeme, result)
    case JsonValue.Str(found)  => appendString(found, result)
    case JsonValue.Arr(values) =>
      appendRaw('[', result)
      var index = 0
      while index < values.length do
        if index > 0 then appendRaw(',', result)
        append(values(index), result)
        index += 1
      appendRaw(']', result)
    case JsonValue.Obj(found) =>
      appendRaw('{', result)
      val fields = found.fields.sortBy(_._1)
      var index = 0
      while index < fields.length do
        if index > 0 then appendRaw(',', result)
        appendString(fields(index)._1, result)
        appendRaw(':', result)
        append(fields(index)._2, result)
        index += 1
      appendRaw('}', result)

  private def appendString(value: String, result: java.lang.StringBuilder): Unit =
    appendRaw('"', result)
    var index = 0
    while index < value.length do
      value.charAt(index) match
        case '"'                          => appendRaw("\\\"", result)
        case '\\'                         => appendRaw("\\\\", result)
        case '\b'                         => appendRaw("\\b", result)
        case '\f'                         => appendRaw("\\f", result)
        case '\n'                         => appendRaw("\\n", result)
        case '\r'                         => appendRaw("\\r", result)
        case '\t'                         => appendRaw("\\t", result)
        case character if character < ' ' =>
          appendRaw("\\u", result)
          val hex = Integer.toHexString(character.toInt)
          var padding = hex.length
          while padding < 4 do
            appendRaw('0', result)
            padding += 1
          appendRaw(hex, result)
        case character => appendRaw(character, result)
      index += 1
    appendRaw('"', result)

  private def appendRaw(value: String, result: java.lang.StringBuilder): Unit =
    val _ = result.append(value)

  private def appendRaw(value: Char, result: java.lang.StringBuilder): Unit =
    val _ = result.append(value)
