package zarr4s

import java.nio.file.Files
import java.nio.file.Path

object WriterFixtureMain:
  def main(arguments: Array[String]): Unit =
    require(arguments.length == 1, "expected output parent")
    val parent = Path.of(arguments(0))
    Files.createDirectories(parent)
    writeRankFive(parent.resolve("rank5.zarr"))
    writeSharded(parent.resolve("sharded.zarr"))
    writeUInt64(parent.resolve("uint64.zarr"))
    writeTransposeV2(parent.resolve("transpose-v2.zarr"))
    writeV2Gzip(parent.resolve("v2-gzip.zarr"))
    writeScalars(parent)
    writeFacadeDirect(parent.resolve("facade-direct.zarr"))
    writeFacadeBorder(parent.resolve("facade-border.zarr"))
    writeFacadeFill(parent.resolve("facade-fill.zarr"))
    writeFacadeSharded(parent.resolve("facade-sharded.zarr"))
    writeFacadeV2(parent.resolve("facade-v2.zarr"))

  private def writeFacadeDirect(target: Path): Unit =
    val shape = Shape(2L, 3L).toOption.get
    val spec = ArraySpec(DType.Int16, shape, shape).toOption.get
    val data = DenseArray
      .copyOf(DType.Int16, shape, Array[Short](1, 2, 3, 4, 5, 6))
      .toOption
      .get
    writeTyped(target, spec, data)

  private def writeFacadeBorder(target: Path): Unit =
    val shape = Shape(3L, 4L).toOption.get
    val chunks = Shape(2L, 3L).toOption.get
    val spec = ArraySpec(DType.Int16, shape, chunks).toOption.get
    val data = DenseArray
      .copyOf(DType.Int16, shape, Array.tabulate[Short](12)(index => (index + 1).toShort))
      .toOption
      .get
    writeTyped(target, spec, data)

  private def writeFacadeFill(target: Path): Unit =
    val shape = Shape(3L, 4L).toOption.get
    val chunks = Shape(2L, 3L).toOption.get
    val spec = ArraySpec(DType.Int16, shape, chunks).toOption.get.withFill(7.toShort)
    writeTypedFill(target, spec)

  private def writeFacadeSharded(target: Path): Unit =
    val shape = Shape(4L, 4L).toOption.get
    val spec = ArraySpec(DType.Int16, shape, shape).toOption.get
    val data = DenseArray
      .copyOf(DType.Int16, shape, Array.tabulate[Short](16)(index => (index + 1).toShort))
      .toOption
      .get
    val sharding = ShardingSpec.indexed(Shape(2L, 2L).toOption.get)
    writeTyped(target, spec, data, Some(sharding))

  private def writeFacadeV2(target: Path): Unit =
    val shape = Shape(2L, 3L).toOption.get
    val spec = ArraySpec(DType.Int16, shape, shape).toOption.get.asFormat(ZarrFormat.V2)
    val data = DenseArray
      .copyOf(DType.Int16, shape, Array[Short](7, 8, 9, 10, 11, 12))
      .toOption
      .get
    writeTyped(target, spec, data)

  private def writeTyped[D <: DType](
      target: Path,
      spec: ArraySpec[D],
      data: DenseArray[D],
      sharding: Option[ShardingSpec] = None
  ): Unit =
    JvmZarr.createArray(target, spec, data, sharding = sharding) match
      case Left(error)   => throw new IllegalStateException(error.message)
      case Right(result) =>
        result.outcome match
          case WriteOutcome.Complete(_)          => ()
          case WriteOutcome.Incomplete(_, error) =>
            throw new IllegalStateException(error.message)

  private def writeTypedFill[D <: DType](target: Path, spec: ArraySpec[D]): Unit =
    JvmZarr.createFillArray(target, spec) match
      case Left(error)   => throw new IllegalStateException(error.message)
      case Right(result) =>
        result.outcome match
          case WriteOutcome.Complete(_)          => ()
          case WriteOutcome.Incomplete(_, error) =>
            throw new IllegalStateException(error.message)

  private def writeRankFive(target: Path): Unit =
    val metadata =
      """{"zarr_format":3,"node_type":"array","shape":[2,2,2,2,3],"data_type":"int16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,2,2,2]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}},{"name":"gzip","configuration":{"level":1}},{"name":"crc32c"}],"dimension_names":["sample","time","z","y","x"],"attributes":{},"storage_transformers":[]}"""
    val descriptor = compile(metadata)
    val provider = linearProvider(descriptor.grid, fillValue = 0)
    write(target, descriptor, provider)

  private def writeSharded(target: Path): Unit =
    val descriptor = compile(ZarrBinaryFixtures.shardedStartMetadata)
    val provider = new ChunkProvider:
      def chunk(coordinate: ChunkCoordinate, storedShape: Shape): Either[ZarrError, ChunkPayload] =
        coordinate.toVector match
          case Vector(0L, 0L) => Right(ChunkPayload.Values(int16(1, 2, 3, 4)))
          case Vector(1L, 1L) => Right(ChunkPayload.Values(int16(13, 14, 15, 16)))
          case _              => Right(ChunkPayload.Fill)
    write(target, descriptor, provider)

  private def writeUInt64(target: Path): Unit =
    val descriptor = compile(
      """{"zarr_format":3,"node_type":"array","shape":[2,3],"data_type":"uint64","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,3]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"big"}}],"attributes":{},"storage_transformers":[]}"""
    )
    val block = PrimitiveBlock.UInt64(
      OwnedLongs.copyOf(
        Array(
          0L,
          1L,
          Long.MaxValue,
          Long.MinValue,
          -2L,
          -1L
        )
      )
    )
    write(target, descriptor, constantProvider(block))

  private def writeTransposeV2(target: Path): Unit =
    val descriptor = compile(
      """{"zarr_format":3,"node_type":"array","shape":[2,3],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,3]}},"chunk_key_encoding":{"name":"v2"},"fill_value":0,"codecs":[{"name":"transpose","configuration":{"order":[1,0]}},{"name":"bytes","configuration":{"endian":"big"}}],"attributes":{},"storage_transformers":[]}"""
    )
    val block = PrimitiveBlock.Int32(OwnedInts.copyOf(Array.range(0, 6)))
    write(target, descriptor, constantProvider(block))

  private def writeV2Gzip(target: Path): Unit =
    val descriptor = compile(
      """{"zarr_format":3,"node_type":"array","shape":[2,3],"data_type":"int16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,3]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}},{"name":"gzip","configuration":{"level":1}}],"dimension_names":["y","x"],"attributes":{"title":"v2 fixture"},"storage_transformers":[]}"""
    )
    write(
      target,
      descriptor,
      constantProvider(
        PrimitiveBlock.Int16(OwnedShorts.copyOf(Array[Short](1, -2, 300, 4, 5, -6)))
      ),
      ZarrFormat.V2
    )

  private def writeScalars(parent: Path): Unit =
    val values = Vector[(String, String, PrimitiveBlock)](
      (
        "bool",
        "false",
        PrimitiveBlock.Bool(
          OwnedBooleans.copyOf(
            Array(false, true, false, true, true, false)
          )
        )
      ),
      (
        "int8",
        "0",
        PrimitiveBlock.Int8(
          OwnedBytes.copyOf(
            Array[Byte](Byte.MinValue, -1, 0, 1, 42, Byte.MaxValue)
          )
        )
      ),
      (
        "uint8",
        "0",
        PrimitiveBlock.UInt8(
          OwnedBytes.copyOf(
            Array[Byte](0, 1, 127, -128, -2, -1)
          )
        )
      ),
      (
        "int16",
        "0",
        PrimitiveBlock.Int16(
          OwnedShorts.copyOf(
            Array[Short](Short.MinValue, -1, 0, 1, 42, Short.MaxValue)
          )
        )
      ),
      (
        "uint16",
        "0",
        PrimitiveBlock.UInt16(
          OwnedShorts.copyOf(
            Array[Short](0, 1, 32767, -32768, -2, -1)
          )
        )
      ),
      (
        "int32",
        "0",
        PrimitiveBlock.Int32(
          OwnedInts.copyOf(
            Array(Int.MinValue, -1, 0, 1, 42, Int.MaxValue)
          )
        )
      ),
      (
        "uint32",
        "0",
        PrimitiveBlock.UInt32(
          OwnedInts.copyOf(
            Array(0, 1, Int.MaxValue, Int.MinValue, -2, -1)
          )
        )
      ),
      (
        "int64",
        "0",
        PrimitiveBlock.Int64(
          OwnedLongs.copyOf(
            Array(Long.MinValue, -1L, 0L, 1L, 42L, Long.MaxValue)
          )
        )
      ),
      (
        "uint64",
        "0",
        PrimitiveBlock.UInt64(
          OwnedLongs.copyOf(
            Array(0L, 1L, Long.MaxValue, Long.MinValue, -2L, -1L)
          )
        )
      ),
      (
        "float32",
        "0.0",
        PrimitiveBlock.Float32(
          OwnedFloats.copyOf(
            Array(0.0f, -0.0f, 1.5f, -2.25f, Float.PositiveInfinity, Float.NegativeInfinity)
          )
        )
      ),
      (
        "float64",
        "0.0",
        PrimitiveBlock.Float64(
          OwnedDoubles.copyOf(
            Array(0.0, -0.0, 1.5, -2.25, Double.PositiveInfinity, Double.NegativeInfinity)
          )
        )
      )
    )
    values.foreach: (name, fill, block) =>
      val configuration =
        if name == "bool" || name.endsWith("8") then ""
        else "\"configuration\":{\"endian\":\"big\"},"
      val descriptor = compile(
        s"""{"zarr_format":3,"node_type":"array","shape":[2,3],"data_type":"$name","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,3]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":$fill,"codecs":[{${configuration}"name":"bytes"}],"attributes":{},"storage_transformers":[]}"""
      )
      write(parent.resolve(s"scala-$name.zarr"), descriptor, constantProvider(block))

  private def constantProvider(block: PrimitiveBlock): ChunkProvider = new ChunkProvider:
    def chunk(coordinate: ChunkCoordinate, storedShape: Shape): Either[ZarrError, ChunkPayload] =
      Right(ChunkPayload.Values(block))

  private def linearProvider(grid: RegularGrid, fillValue: Short): ChunkProvider =
    new ChunkProvider:
      def chunk(coordinate: ChunkCoordinate, storedShape: Shape): Either[ZarrError, ChunkPayload] =
        val count = storedShape.elementCount.fold(
          error => throw new IllegalStateException(error.message),
          identity
        )
        val values = Array.fill[Short](count.toInt)(fillValue)
        val cursor = new Array[Long](storedShape.rank.toInt)
        var element = 0
        while element < values.length do
          var linear = 0L
          var axis = 0
          var inside = true
          while axis < cursor.length do
            val global = coordinate.axis(axis) * grid.chunkShape.axis(axis) + cursor(axis)
            linear = linear * grid.arrayShape.axis(axis) + global
            if global >= grid.arrayShape.axis(axis) then inside = false
            axis += 1
          if inside then values(element) = linear.toShort
          advance(cursor, storedShape)
          element += 1
        Right(ChunkPayload.Values(PrimitiveBlock.Int16(OwnedShorts.copyOf(values))))

  private def advance(cursor: Array[Long], shape: Shape): Unit =
    var axis = cursor.length - 1
    var advanced = false
    while axis >= 0 && !advanced do
      cursor(axis) += 1L
      if cursor(axis) < shape.axis(axis) then advanced = true
      else
        cursor(axis) = 0L
        axis -= 1

  private def compile(metadata: String): ArrayDescriptor =
    val array = ZarrMetadata.parse(metadata) match
      case Right(ZarrNodeMetadata.Array(found)) => found
      case Right(_)    => throw new IllegalArgumentException("expected array metadata")
      case Left(error) => throw new IllegalArgumentException(error.message)
    ArrayDescriptor.compile(array) match
      case Right(found) => found
      case Left(error)  => throw new IllegalArgumentException(error.message)

  private def int16(values: Short*): PrimitiveBlock =
    PrimitiveBlock.Int16(OwnedShorts.copyOf(values.toArray))

  private def write(
      target: Path,
      descriptor: ArrayDescriptor,
      provider: ChunkProvider,
      format: ZarrFormat = ZarrFormat.V3
  ): Unit =
    JvmZarrWriter.create(target, descriptor, provider, format = format) match
      case Right(_)    => ()
      case Left(error) => throw new IllegalStateException(error.message)
