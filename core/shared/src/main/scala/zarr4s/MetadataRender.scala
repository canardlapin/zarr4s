package zarr4s

object ZarrMetadataRenderer:
  def group(metadata: GroupMetadata): Either[ZarrError, String] =
    for
      known <- JsonObject
        .from(
          Vector(
            "zarr_format" -> number(3L),
            "node_type" -> JsonValue.Str("group"),
            "attributes" -> JsonValue.Obj(metadata.attributes)
          )
        )
        .left
        .map(detail => ZarrError.InvalidMetadata("$", detail))
      merged <- JsonObject
        .from(known.fields ++ metadata.unknown.fields.filterNot: (name, _) =>
          known.contains(name))
        .left
        .map(detail => ZarrError.InvalidMetadata("$", detail))
    yield JsonValue.Obj(merged).render

  def v2Group(metadata: GroupMetadata): Either[ZarrError, String] =
    val unknown = metadata.unknown.fields.filterNot(_._1 == "zarr_format")
    JsonObject
      .from(Vector("zarr_format" -> number(2L)) ++ unknown)
      .left
      .map(detail => ZarrError.InvalidMetadata("$.zgroup", detail))
      .map(value => JsonValue.Obj(value).render)

  def v2GroupAttributes(metadata: GroupMetadata): Either[ZarrError, String] =
    Right(JsonValue.Obj(metadata.attributes).render)

  def v2Attributes(descriptor: ArrayDescriptor): Either[ZarrError, String] =
    v2AttributesObject(descriptor).map(value => JsonValue.Obj(value).render)

  def v2Array(descriptor: ArrayDescriptor): Either[ZarrError, String] =
    descriptor.layout match
      case PhysicalLayout.Sharded(_, _, _, _, _) =>
        Left(ZarrError.UnsupportedWrite("v2 metadata cannot represent sharding_indexed"))
      case PhysicalLayout.Direct(codecs) =>
        v2Program(descriptor.dataType, descriptor.shape, codecs).flatMap:
          case (dtype, order, filters, compressor) =>
            v2AttributesObject(descriptor).flatMap: _ =>
              val known = Vector(
                "zarr_format" -> number(2L),
                "shape" -> longArray(descriptor.shape.toVector),
                "chunks" -> longArray(descriptor.grid.chunkShape.toVector),
                "dtype" -> JsonValue.Str(dtype),
                "compressor" -> compressor.getOrElse(JsonValue.Null),
                "fill_value" -> fillValue(descriptor.fillValue),
                "order" -> JsonValue.Str(order),
                "filters" ->
                  (if filters.isEmpty then JsonValue.Null else JsonValue.Arr(filters)),
                "dimension_separator" -> JsonValue.Str(
                  descriptor.chunkKeyEncoding.separator match
                    case ChunkSeparator.Dot   => "."
                    case ChunkSeparator.Slash => "/"
                )
              )
              JsonObject
                .from(known)
                .left
                .map(detail => ZarrError.InvalidMetadata("$.zarray", detail))
                .flatMap: value =>
                  JsonObject
                    .from(value.fields ++ descriptor.unknown.fields.filterNot: (name, _) =>
                      value.contains(name))
                    .left
                    .map(detail => ZarrError.InvalidMetadata("$.zarray", detail))
                    .map(merged => JsonValue.Obj(merged).render)

  def array(descriptor: ArrayDescriptor): Either[ZarrError, String] =
    for
      chunkGrid <- renderExtension(
        "regular",
        Vector(
          "chunk_shape" -> longArray(descriptor.grid.chunkShape.toVector)
        )
      )
      chunkKey <- renderExtension(
        descriptor.chunkKeyEncoding.name,
        Vector(
          "separator" -> JsonValue.Str(descriptor.chunkKeyEncoding.separator match
            case ChunkSeparator.Slash => "/"
            case ChunkSeparator.Dot   => ".")
        )
      )
      codecs <- layoutCodecs(descriptor.layout)
      known <- JsonObject
        .from(
          Vector(
            "zarr_format" -> number(3L),
            "node_type" -> JsonValue.Str("array"),
            "shape" -> longArray(descriptor.shape.toVector),
            "data_type" -> JsonValue.Str(descriptor.dataType.name),
            "chunk_grid" -> chunkGrid,
            "chunk_key_encoding" -> chunkKey,
            "fill_value" -> fillValue(descriptor.fillValue),
            "codecs" -> JsonValue.Arr(codecs),
            "attributes" -> JsonValue.Obj(descriptor.attributes),
            "storage_transformers" -> JsonValue.Arr(Vector.empty)
          ) ++ descriptor.dimensionNames.toVector.map: names =>
            "dimension_names" -> JsonValue.Arr(names.map:
              case None       => JsonValue.Null
              case Some(name) => JsonValue.Str(name))
        )
        .left
        .map(detail => ZarrError.InvalidMetadata("$", detail))
      merged <- JsonObject
        .from(known.fields ++ descriptor.unknown.fields.filterNot: (name, _) =>
          known.contains(name))
        .left
        .map(detail => ZarrError.InvalidMetadata("$", detail))
    yield JsonValue.Obj(merged).render

  private def layoutCodecs(layout: PhysicalLayout): Either[ZarrError, Vector[JsonValue]] =
    layout match
      case PhysicalLayout.Direct(codecs) => compiledCodecs(codecs)
      case PhysicalLayout.Sharded(sharded, innerCodecs, indexCodecs, location, outerCodecs) =>
        for
          inner <- compiledCodecs(innerCodecs)
          index <- compiledCodecs(indexCodecs.codecs)
          outer <- compiledCodecs(outerCodecs)
          sharding <- renderExtension(
            "sharding_indexed",
            Vector(
              "chunk_shape" -> longArray(sharded.innerChunkShape.toVector),
              "codecs" -> JsonValue.Arr(inner),
              "index_codecs" -> JsonValue.Arr(index),
              "index_location" -> JsonValue.Str(location match
                case IndexLocation.Start => "start"
                case IndexLocation.End   => "end")
            )
          )
        yield sharding +: outer

  private def v2Program(
      dataType: DataTypeCapability,
      shape: Shape,
      codecs: CodecProgram
  ): Either[ZarrError, (String, String, Vector[JsonValue], Option[JsonValue])] =
    var byteOrder: Option[Endianness] = None
    var dtypeText: Option[String] = None
    var order = "C"
    var transposeSeen = false
    var deltaSeen = false
    var byteSeen = false
    var compressor: Option[JsonValue] = None
    val filters = Vector.newBuilder[JsonValue]
    var index = 0
    while index < codecs.stages.length do
      codecs.stages(index) match
        case transpose: TransposeCodec =>
          if transposeSeen || deltaSeen || byteSeen then
            return Left(ZarrError.InvalidCodecChain("v2 transpose must be the first array codec"))
          val expected = shape.rank.toInt match
            case 0    => Vector.empty[Int]
            case rank => (rank - 1 to 0 by -1).toVector
          if transpose.order != expected then
            return Left(
              ZarrError.UnsupportedWrite(
                "v2 order only represents C or reverse-axis F transpose"
              )
            )
          order = "F"
          transposeSeen = true
        case delta: DeltaCodec =>
          if deltaSeen || byteSeen || compressor.nonEmpty then
            return Left(ZarrError.InvalidCodecChain("v2 delta filter must precede bytes"))
          dtypeText = Some(delta.dtype)
          extensionValue("delta", delta.configuration.fields) match
            case Left(error)  => return Left(error)
            case Right(value) => filters += value
          deltaSeen = true
        case BytesCodec(endianness) =>
          if byteSeen then
            return Left(
              ZarrError.InvalidCodecChain("v2 codec program contains multiple bytes stages")
            )
          byteSeen = true
          byteOrder = endianness
        case shuffle: ShuffleCodec =>
          if !byteSeen || compressor.nonEmpty then
            return Left(
              ZarrError.InvalidCodecChain("v2 shuffle must follow bytes and precede compression")
            )
          extensionValue("shuffle", shuffle.configuration.fields) match
            case Left(error)  => return Left(error)
            case Right(value) => filters += value
        case stage
            if stage.input == CodecRepresentation.Bytes && stage.output == CodecRepresentation.Bytes =>
          if !byteSeen || compressor.nonEmpty then
            return Left(ZarrError.InvalidCodecChain("v2 supports at most one byte compressor"))
          v2CompressorValue(stage.name, stage.configuration.fields) match
            case Left(error)  => return Left(error)
            case Right(value) => compressor = Some(value)
        case stage =>
          return Left(ZarrError.UnsupportedWrite(s"v2 codec ${stage.name} is not representable"))
      index += 1
    if !byteSeen then Left(ZarrError.InvalidCodecChain("v2 codec program is missing bytes"))
    else
      v2Dtype(dataType, dtypeText, byteOrder).map(dtype =>
        (dtype, order, filters.result(), compressor)
      )

  private def v2Dtype(
      dataType: DataTypeCapability,
      original: Option[String],
      endianness: Option[Endianness]
  ): Either[ZarrError, String] = original match
    case Some(value) => Right(value)
    case None        =>
      val kind = dataType.name match
        case "bool"       => "b"
        case "int8"       => "i"
        case "int16"      => "i"
        case "int32"      => "i"
        case "int64"      => "i"
        case "uint8"      => "u"
        case "uint16"     => "u"
        case "uint32"     => "u"
        case "uint64"     => "u"
        case "float16"    => "f"
        case "float32"    => "f"
        case "float64"    => "f"
        case "complex64"  => "c"
        case "complex128" => "c"
        case found        => return Left(ZarrError.UnsupportedDataType(s"v2 dtype $found"))
      val prefix =
        if dataType.byteWidth == 1 then "|"
        else
          endianness match
            case Some(Endianness.Little) => "<"
            case Some(Endianness.Big)    => ">"
            case None                    =>
              return Left(
                ZarrError.InvalidCodecChain(
                  s"v2 multibyte dtype ${dataType.name} requires explicit byte order"
                )
              )
      Right(s"$prefix$kind${dataType.byteWidth}")

  private def extensionValue(
      name: String,
      configuration: Vector[(String, JsonValue)]
  ): Either[ZarrError, JsonValue] =
    JsonObject
      .from(Vector("id" -> JsonValue.Str(name)) ++ configuration)
      .left
      .map(detail => ZarrError.InvalidMetadata("$.zarray", detail))
      .map(JsonValue.Obj.apply)

  private def v2CompressorValue(
      name: String,
      configuration: Vector[(String, JsonValue)]
  ): Either[ZarrError, JsonValue] =
    if name != "blosc" then extensionValue(name, configuration)
    else
      val shuffle = configuration.find(_._1 == "shuffle") match
        case None                                   => Right(0L)
        case Some((_, JsonValue.Str("noshuffle")))  => Right(0L)
        case Some((_, JsonValue.Str("shuffle")))    => Right(1L)
        case Some((_, JsonValue.Str("bitshuffle"))) => Right(2L)
        case Some((_, JsonValue.Num(value)))        => value.toLongExact
        case Some(_) => Left("v2 Blosc shuffle must be 'noshuffle', 'shuffle', or 'bitshuffle'")
      shuffle.left
        .map(detail => ZarrError.InvalidMetadata("$.zarray.compressor.shuffle", detail))
        .flatMap: value =>
          if value < 0L || value > 2L then
            Left(ZarrError.InvalidMetadata("$.zarray.compressor.shuffle", "must be 0, 1, or 2"))
          else
            val fields =
              Vector("id" -> JsonValue.Str("blosc")) ++
                configuration.filterNot(_._1 == "shuffle") ++
                Vector("shuffle" -> number(value))
            JsonObject
              .from(fields)
              .left
              .map(detail => ZarrError.InvalidMetadata("$.zarray.compressor", detail))
              .map(JsonValue.Obj.apply)

  private def v2AttributesObject(descriptor: ArrayDescriptor): Either[ZarrError, JsonObject] =
    descriptor.dimensionNames match
      case None                                    => Right(descriptor.attributes)
      case Some(names) if names.forall(_.nonEmpty) =>
        val dimensions = JsonValue.Arr(names.map(_.map(JsonValue.Str.apply).get))
        JsonObject
          .from(
            descriptor.attributes.fields.filterNot(_._1 == "_ARRAY_DIMENSIONS") ++
              Vector("_ARRAY_DIMENSIONS" -> dimensions)
          )
          .left
          .map(detail => ZarrError.InvalidMetadata("$.zattrs", detail))
      case Some(_) =>
        Left(ZarrError.InvalidMetadata("$.zattrs._ARRAY_DIMENSIONS", "names must be strings"))

  private def compiledCodecs(codecs: CodecProgram): Either[ZarrError, Vector[JsonValue]] =
    val result = Vector.newBuilder[JsonValue]
    var index = 0
    while index < codecs.stages.length do
      val codec = codecs.stages(index)
      val rendered = renderExtension(codec.name, codec.configuration.fields)
      rendered match
        case Left(error)  => return Left(error)
        case Right(value) => result += value
      index += 1
    Right(result.result())

  private def renderExtension(
      name: String,
      configuration: Vector[(String, JsonValue)]
  ): Either[ZarrError, JsonValue] =
    JsonObject
      .from(configuration)
      .left
      .map: detail =>
        ZarrError.InvalidMetadata("$.configuration", detail)
      .flatMap: config =>
        val fields = Vector.newBuilder[(String, JsonValue)]
        fields += "name" -> JsonValue.Str(name)
        if configuration.nonEmpty then fields += "configuration" -> JsonValue.Obj(config)
        JsonObject
          .from(fields.result())
          .left
          .map: detail =>
            ZarrError.InvalidMetadata("$.extension", detail)
          .map(JsonValue.Obj.apply)

  private def fillValue(fill: StoredScalar): JsonValue = fill match
    case StoredScalar.Boolean(value)          => JsonValue.Bool(value)
    case StoredScalar.Integral(value)         => number(value)
    case StoredScalar.UnsignedIntegral(value) =>
      JsonValue.Num(JsonNumber.unsafe(value.toString))
    case value: StoredScalar.Floating          => floatingValue(value)
    case StoredScalar.FloatingBits(hex)        => JsonValue.Str(hex)
    case StoredScalar.Complex(real, imaginary) =>
      JsonValue.Arr(Vector(floatingComponent(real), floatingComponent(imaginary)))
    case StoredScalar.RawBytes(values) =>
      JsonValue.Arr(values.map(value => number(value.toLong)))

  private def floatingValue(value: StoredScalar.Floating): JsonValue =
    if value.value.isNaN then JsonValue.Str("NaN")
    else if value.value == Double.PositiveInfinity then JsonValue.Str("Infinity")
    else if value.value == Double.NegativeInfinity then JsonValue.Str("-Infinity")
    else JsonValue.Num(JsonNumber.unsafe(java.lang.Double.toString(value.value)))

  private def floatingComponent(value: StoredFloating): JsonValue = value match
    case StoredFloating.Value(found) if found.isNaN                      => JsonValue.Str("NaN")
    case StoredFloating.Value(found) if found == Double.PositiveInfinity =>
      JsonValue.Str("Infinity")
    case StoredFloating.Value(found) if found == Double.NegativeInfinity =>
      JsonValue.Str("-Infinity")
    case StoredFloating.Value(found) =>
      JsonValue.Num(JsonNumber.unsafe(java.lang.Double.toString(found)))
    case StoredFloating.Bits(hex) => JsonValue.Str(hex)

  private def longArray(values: Vector[Long]): JsonValue =
    JsonValue.Arr(values.map(number))

  private def number(value: Long): JsonValue =
    JsonValue.Num(JsonNumber.unsafe(value.toString))
