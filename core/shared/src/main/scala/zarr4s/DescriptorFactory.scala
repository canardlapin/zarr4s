package zarr4s

/** The chunk-key choices exposed by the programmatic descriptor factory. */
enum ChunkKeySpec:
  case Default(separator: ChunkSeparator)
  case V2(separator: ChunkSeparator)

object ChunkKeySpec:
  val v3Default: ChunkKeySpec = Default(ChunkSeparator.Slash)
  val v2Default: ChunkKeySpec = V2(ChunkSeparator.Dot)

/** Codec intent accepted by the high-level descriptor factory.
  *
  * The compiler still owns all representation and capability validation. These values only make the
  * common extension choices constructible without JSON.
  */
sealed trait ArrayCodecSpec

object ArrayCodecSpec:
  final case class Bytes(endianness: Option[Endianness]) extends ArrayCodecSpec

  object Bytes:
    val little: Bytes = Bytes(Some(Endianness.Little))
    val big: Bytes = Bytes(Some(Endianness.Big))
    val native: Bytes = Bytes(None)

  final case class Gzip(level: Int) extends ArrayCodecSpec

  final case class Zlib(level: Int) extends ArrayCodecSpec

  final case class Shuffle(elementSize: Int) extends ArrayCodecSpec

  final case class Transpose(order: Vector[Int]) extends ArrayCodecSpec

  case object Crc32c extends ArrayCodecSpec

/** The v3 indexed-sharding profile accepted by [[ArrayDescriptor.sharded]]. */
final case class ShardingSpec(
    innerChunkShape: Shape,
    innerCodecs: Vector[ArrayCodecSpec],
    indexCodecs: Vector[ArrayCodecSpec],
    indexLocation: IndexLocation
)

object ShardingSpec:
  def indexed(
      innerChunkShape: Shape,
      innerCodecs: Vector[ArrayCodecSpec] = Vector(ArrayCodecSpec.Bytes.little),
      indexCodecs: Vector[ArrayCodecSpec] =
        Vector(ArrayCodecSpec.Bytes.little, ArrayCodecSpec.Crc32c),
      indexLocation: IndexLocation = IndexLocation.End
  ): ShardingSpec =
    ShardingSpec(innerChunkShape, innerCodecs, indexCodecs, indexLocation)

private[zarr4s] object ProgrammaticDescriptor:
  def direct[D <: DType](
      spec: ArraySpec[D],
      codecs: Vector[ArrayCodecSpec],
      chunkKey: Option[ChunkKeySpec],
      capabilities: ZarrCapabilities
  ): Either[ZarrError, ArrayDescriptor] = spec.format match
    case ZarrFormat.V3 => compileV3(spec, codecs, chunkKey, None, capabilities)
    case ZarrFormat.V2 => compileV2(spec, codecs, chunkKey, capabilities)

  def sharded[D <: DType](
      spec: ArraySpec[D],
      sharding: ShardingSpec,
      outerCodecs: Vector[ArrayCodecSpec],
      chunkKey: Option[ChunkKeySpec],
      capabilities: ZarrCapabilities
  ): Either[ZarrError, ArrayDescriptor] = spec.format match
    case ZarrFormat.V2 =>
      Left(ZarrError.UnsupportedWrite("indexed sharding is a Zarr v3 descriptor feature"))
    case ZarrFormat.V3 => compileV3(spec, outerCodecs, chunkKey, Some(sharding), capabilities)

  private def compileV3[D <: DType](
      spec: ArraySpec[D],
      codecs: Vector[ArrayCodecSpec],
      chunkKey: Option[ChunkKeySpec],
      sharding: Option[ShardingSpec],
      capabilities: ZarrCapabilities
  ): Either[ZarrError, ArrayDescriptor] =
    for
      chunkGrid <- makeExtension(
        "regular",
        Vector("chunk_shape" -> shapeValue(spec.chunkShape))
      )
      keyEncoding <- v3ChunkKey(chunkKey)
      codecExtensions <- codecExtensions(codecs, defaultBytes = sharding.isEmpty)
      shardingExtension <- sharding match
        case None        => Right(Vector.empty[ExtensionMetadata])
        case Some(found) => shardingExtensions(found)
      fill <- fillValue(spec)
      metadata = ArrayMetadata(
        spec.shape.toVector,
        ExtensionMetadata(spec.dtype.name, JsonObject.empty, true, JsonObject.empty),
        chunkGrid,
        keyEncoding,
        fill,
        shardingExtension ++ codecExtensions,
        spec.attributes,
        spec.dimensionNames,
        Vector.empty,
        JsonObject.empty
      )
      descriptor <- ArrayDescriptor.compile(metadata, capabilities)
    yield descriptor

  private def compileV2[D <: DType](
      spec: ArraySpec[D],
      codecs: Vector[ArrayCodecSpec],
      chunkKey: Option[ChunkKeySpec],
      capabilities: ZarrCapabilities
  ): Either[ZarrError, ArrayDescriptor] =
    for
      dtype <- v2DType(spec.dtype)
      attributes <- v2Attributes(spec)
      codecParts <- v2Codecs(codecs, spec.shape.rank.toInt)
      separator <- v2Separator(chunkKey)
      fill <- fillValue(spec)
      metadata = V2ArrayMetadata(
        spec.shape.toVector,
        spec.chunkShape.toVector,
        dtype,
        codecParts.compressor,
        fill,
        codecParts.order,
        codecParts.filters,
        separator,
        attributes,
        JsonObject.empty
      )
      descriptor <- V2ArrayDescriptor.compile(metadata, capabilities)
    yield descriptor

  private def fillValue[D <: DType](spec: ArraySpec[D]): Either[ZarrError, JsonValue] =
    Right(spec.fillValue.fold(spec.dtype.zeroJson)(value => spec.dtype.jsonFill(value)))

  private def makeExtension(
      name: String,
      fields: Vector[(String, JsonValue)]
  ): Either[ZarrError, ExtensionMetadata] =
    JsonObject
      .from(fields)
      .left
      .map(detail => ZarrError.InvalidMetadata("$.configuration", detail))
      .map(configuration => ExtensionMetadata(name, configuration, true, JsonObject.empty))

  private def v3ChunkKey(chunkKey: Option[ChunkKeySpec]): Either[ZarrError, ExtensionMetadata] =
    chunkKey.getOrElse(ChunkKeySpec.v3Default) match
      case ChunkKeySpec.Default(separator) =>
        makeExtension("default", Vector("separator" -> separatorValue(separator)))
      case ChunkKeySpec.V2(separator) =>
        makeExtension("v2", Vector("separator" -> separatorValue(separator)))

  private def v2Separator(chunkKey: Option[ChunkKeySpec]): Either[ZarrError, ChunkSeparator] =
    chunkKey.getOrElse(ChunkKeySpec.v2Default) match
      case ChunkKeySpec.Default(separator) => Right(separator)
      case ChunkKeySpec.V2(separator)      => Right(separator)

  private def codecExtensions(
      codecs: Vector[ArrayCodecSpec],
      defaultBytes: Boolean
  ): Either[ZarrError, Vector[ExtensionMetadata]] =
    val selected =
      if codecs.isEmpty && defaultBytes then Vector(ArrayCodecSpec.Bytes.little) else codecs
    val result = Vector.newBuilder[ExtensionMetadata]
    var index = 0
    while index < selected.length do
      codecExtension(selected(index)) match
        case Left(error)  => return Left(error)
        case Right(value) => result += value
      index += 1
    Right(result.result())

  private def codecExtension(codec: ArrayCodecSpec): Either[ZarrError, ExtensionMetadata] =
    codec match
      case ArrayCodecSpec.Bytes(endianness) =>
        makeExtension(
          "bytes",
          endianness.map(value => "endian" -> endianValue(value)).toVector
        )
      case ArrayCodecSpec.Gzip(level) =>
        makeExtension("gzip", Vector("level" -> numberValue(level)))
      case ArrayCodecSpec.Zlib(level) =>
        makeExtension("zlib", Vector("level" -> numberValue(level)))
      case ArrayCodecSpec.Shuffle(elementSize) =>
        makeExtension("shuffle", Vector("elementsize" -> numberValue(elementSize)))
      case ArrayCodecSpec.Transpose(order) =>
        makeExtension(
          "transpose",
          Vector("order" -> jsonArray(order.map(value => numberValue(value))))
        )
      case ArrayCodecSpec.Crc32c => makeExtension("crc32c", Vector.empty)

  private def shardingExtensions(
      sharding: ShardingSpec
  ): Either[ZarrError, Vector[ExtensionMetadata]] =
    for
      inner <- codecExtensions(sharding.innerCodecs, defaultBytes = true)
      index <- codecExtensions(sharding.indexCodecs, defaultBytes = true)
      configuration <- JsonObject
        .from(
          Vector(
            "chunk_shape" -> shapeValue(sharding.innerChunkShape),
            "codecs" -> jsonArray(inner.map(extensionValue)),
            "index_codecs" -> jsonArray(index.map(extensionValue)),
            "index_location" -> JsonValue.Str(sharding.indexLocation match
              case IndexLocation.Start => "start"
              case IndexLocation.End   => "end")
          )
        )
        .left
        .map(detail => ZarrError.InvalidMetadata("$.codecs[0].configuration", detail))
    yield Vector(ExtensionMetadata("sharding_indexed", configuration, true, JsonObject.empty))

  private final case class V2CodecParts(
      compressor: Option[JsonObject],
      filters: Vector[JsonObject],
      order: V2MemoryOrder
  )

  private def v2Codecs(
      codecs: Vector[ArrayCodecSpec],
      rank: Int
  ): Either[ZarrError, V2CodecParts] =
    var compressor: Option[JsonObject] = None
    val filters = Vector.newBuilder[JsonObject]
    var order = V2MemoryOrder.C
    var index = 0
    while index < codecs.length do
      codecs(index) match
        case ArrayCodecSpec.Bytes(_) =>
          return Left(
            ZarrError.UnsupportedWrite("v2 bytes is implicit and cannot be configured as a codec")
          )
        case ArrayCodecSpec.Transpose(found) if found == (0 until rank).reverse.toVector =>
          order = V2MemoryOrder.F
        case ArrayCodecSpec.Transpose(_) =>
          return Left(ZarrError.UnsupportedWrite("v2 supports only C or F memory order"))
        case ArrayCodecSpec.Shuffle(elementSize) =>
          filters += codecObject("shuffle", Vector("elementsize" -> numberValue(elementSize)))
        case ArrayCodecSpec.Gzip(level) =>
          compressor = Some(codecObject("gzip", Vector("level" -> numberValue(level))))
        case ArrayCodecSpec.Zlib(level) =>
          compressor = Some(codecObject("zlib", Vector("level" -> numberValue(level))))
        case ArrayCodecSpec.Crc32c => compressor = Some(codecObject("crc32c", Vector.empty))
      index += 1
    Right(V2CodecParts(compressor, filters.result(), order))

  private def v2Attributes[D <: DType](spec: ArraySpec[D]): Either[ZarrError, JsonObject] =
    spec.dimensionNames match
      case None        => Right(spec.attributes)
      case Some(names) =>
        if spec.attributes.contains("_ARRAY_DIMENSIONS") then
          Left(
            ZarrError.InvalidMetadata(
              "$.attributes._ARRAY_DIMENSIONS",
              "dimension names are specified both in attributes and ArraySpec"
            )
          )
        else
          JsonObject
            .from(
              spec.attributes.fields ++ Vector(
                "_ARRAY_DIMENSIONS" -> JsonValue.Arr(
                  names.map(_.fold[JsonValue](JsonValue.Null)(JsonValue.Str.apply))
                )
              )
            )
            .left
            .map(detail => ZarrError.InvalidMetadata("$.attributes", detail))

  private def v2DType(dtype: DType): Either[ZarrError, String] =
    val kind = dtype.dataType.scalarKind match
      case ScalarKind.Bool       => "b"
      case ScalarKind.Signed8    => "i"
      case ScalarKind.Unsigned8  => "u"
      case ScalarKind.Signed16   => "i"
      case ScalarKind.Unsigned16 => "u"
      case ScalarKind.Signed32   => "i"
      case ScalarKind.Unsigned32 => "u"
      case ScalarKind.Signed64   => "i"
      case ScalarKind.Unsigned64 => "u"
      case ScalarKind.Float16    => "f"
      case ScalarKind.Float32    => "f"
      case ScalarKind.Float64    => "f"
      case ScalarKind.Complex64  => "c"
      case ScalarKind.Complex128 => "c"
      case ScalarKind.Raw(width) =>
        return Left(ZarrError.UnsupportedDataType(s"v2 raw width $width"))
    val order = if dtype.dataType.byteWidth == 1 then "|" else "<"
    Right(s"$order$kind${dtype.dataType.byteWidth}")

  private def shapeValue(shape: Shape): JsonValue =
    jsonArray(shape.toVector.map(numberValue))

  private def separatorValue(separator: ChunkSeparator): JsonValue =
    JsonValue.Str(separator match
      case ChunkSeparator.Slash => "/"
      case ChunkSeparator.Dot   => ".")

  private def endianValue(value: Endianness): JsonValue = JsonValue.Str(value match
    case Endianness.Little => "little"
    case Endianness.Big    => "big")

  private def numberValue(value: Int): JsonValue = numberValue(value.toLong)

  private def numberValue(value: Long): JsonValue =
    JsonValue.Num(JsonNumber.unsafe(value.toString))

  private def extensionValue(extension: ExtensionMetadata): JsonValue =
    JsonValue.Obj(
      JsonObject.unsafe(
        Vector(
          "name" -> JsonValue.Str(extension.name),
          "configuration" -> JsonValue.Obj(extension.configuration)
        )
      )
    )

  private def codecObject(
      name: String,
      fields: Vector[(String, JsonValue)]
  ): JsonObject =
    JsonObject.unsafe(Vector("id" -> JsonValue.Str(name)) ++ fields)

  private def jsonArray(values: Vector[JsonValue]): JsonValue = JsonValue.Arr(values)
