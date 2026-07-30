package zarr4s

import java.nio.file.Files
import java.nio.file.Path

object PythonInteropReadMain:
  def main(arguments: Array[String]): Unit =
    require(arguments.length == 1, "expected Python fixture parent")
    val parent = Path.of(arguments(0))
    assertValues(
      parent.resolve("python-direct.zarr"),
      Vector[Short](1, -2, 300, 4, 5, -6)
    )
    assertValues(
      parent.resolve("python-sharded.zarr"),
      (1 to 16).map(_.toShort).toVector
    )
    assertCommonScalars(parent)
    assertInt32(
      parent.resolve("python-transpose-v2.zarr"),
      Vector(0, 1, 2, 3, 4, 5)
    )
    assertValues(parent.resolve("python-v2-c.zarr"), Vector[Short](0, 1, 2, 3, 4, 5))
    assertValues(parent.resolve("python-v2-f.zarr"), Vector[Short](0, 1, 2, 3, 4, 5))
    assertValues(parent.resolve("python-v2-gzip.zarr"), Vector[Short](1, -2, 300, 4, 5, -6))
    assertHierarchy(parent.resolve("python-v2-hierarchy.zarr"), ZarrFormat.V2)
    assertHierarchy(parent.resolve("python-v3-hierarchy.zarr"), ZarrFormat.V3)
    assertFactored(parent)

  private def assertValues(path: Path, expected: Vector[Short]): Unit =
    read(path) match
      case PrimitiveBlock.Int16(values) =>
        require(values.toArray.toVector == expected, s"$path values differ")
      case found => throw IllegalStateException(s"expected int16, found $found")

  private def assertCommonScalars(parent: Path): Unit =
    read(parent.resolve("python-bool.zarr")) match
      case PrimitiveBlock.Bool(values) =>
        require(values.toArray.toVector == Vector(false, true, false, true, true, false))
      case found => throw IllegalStateException(s"expected bool, found $found")
    read(parent.resolve("python-int8.zarr")) match
      case PrimitiveBlock.Int8(values) =>
        require(values.toArray.toVector == Vector[Byte](-128, -1, 0, 1, 42, 127))
      case found => throw IllegalStateException(s"expected int8, found $found")
    read(parent.resolve("python-uint8.zarr")) match
      case PrimitiveBlock.UInt8(values) =>
        require(values.toArray.toVector == Vector[Byte](0, 1, 127, -128, -2, -1))
      case found => throw IllegalStateException(s"expected uint8, found $found")
    read(parent.resolve("python-int16.zarr")) match
      case PrimitiveBlock.Int16(values) =>
        require(
          values.toArray.toVector == Vector[Short](Short.MinValue, -1, 0, 1, 42, Short.MaxValue)
        )
      case found => throw IllegalStateException(s"expected int16, found $found")
    read(parent.resolve("python-uint16.zarr")) match
      case PrimitiveBlock.UInt16(values) =>
        require(values.toArray.toVector == Vector[Short](0, 1, 32767, -32768, -2, -1))
      case found => throw IllegalStateException(s"expected uint16, found $found")
    assertInt32(
      parent.resolve("python-int32.zarr"),
      Vector(Int.MinValue, -1, 0, 1, 42, Int.MaxValue)
    )
    read(parent.resolve("python-uint32.zarr")) match
      case PrimitiveBlock.UInt32(values) =>
        require(values.toArray.toVector == Vector(0, 1, Int.MaxValue, Int.MinValue, -2, -1))
      case found => throw IllegalStateException(s"expected uint32, found $found")
    read(parent.resolve("python-int64.zarr")) match
      case PrimitiveBlock.Int64(values) =>
        require(values.toArray.toVector == Vector(Long.MinValue, -1L, 0L, 1L, 42L, Long.MaxValue))
      case found => throw IllegalStateException(s"expected int64, found $found")
    read(parent.resolve("python-uint64.zarr")) match
      case PrimitiveBlock.UInt64(values) =>
        require(values.toArray.toVector == Vector(0L, 1L, Long.MaxValue, Long.MinValue, -2L, -1L))
      case found => throw IllegalStateException(s"expected uint64, found $found")
    read(parent.resolve("python-float32.zarr")) match
      case PrimitiveBlock.Float32(values) =>
        val expected =
          Array(0.0f, -0.0f, 1.5f, -2.25f, Float.PositiveInfinity, Float.NegativeInfinity)
        require(
          values.toArray
            .map(java.lang.Float.floatToRawIntBits)
            .sameElements(
              expected.map(java.lang.Float.floatToRawIntBits)
            )
        )
      case found => throw IllegalStateException(s"expected float32, found $found")
    read(parent.resolve("python-float64.zarr")) match
      case PrimitiveBlock.Float64(values) =>
        val expected =
          Array(0.0, -0.0, 1.5, -2.25, Double.PositiveInfinity, Double.NegativeInfinity)
        require(
          values.toArray
            .map(java.lang.Double.doubleToRawLongBits)
            .sameElements(
              expected.map(java.lang.Double.doubleToRawLongBits)
            )
        )
      case found => throw IllegalStateException(s"expected float64, found $found")

  private def assertInt32(path: Path, expected: Vector[Int]): Unit = read(path) match
    case PrimitiveBlock.Int32(values) =>
      require(values.toArray.toVector == expected, s"$path values differ")
    case found => throw IllegalStateException(s"expected int32, found $found")

  private def assertHierarchy(path: Path, expectedFormat: ZarrFormat): Unit =
    val store = JvmFileStore
      .open(path)
      .fold(detail => throw IllegalArgumentException(detail), identity)
    val root = SyncZarr
      .openGroup(
        store,
        runtime = JvmCodecRuntime.portable,
        consolidation = ConsolidationMode.Require
      )
      .fold(error => throw IllegalStateException(error.message), identity)
    require(root.format == expectedFormat, s"$path format differs")
    require(
      root.children
        .fold(error => throw IllegalStateException(error.message), identity)
        .map(_.path.value) == Vector("bold", "derived"),
      s"$path root children differ"
    )
    val bold = root
      .openArray("bold")
      .fold(error => throw IllegalStateException(error.message), identity)
    val values = readOpened(bold) match
      case PrimitiveBlock.Int16(found) => found.toArray.toVector
      case found => throw IllegalStateException(s"expected int16, found $found")
    require(values == Vector[Short](0, 1, 2, 3, 4, 5), s"$path bold values differ")
    val derived = root
      .openGroup("derived")
      .fold(error => throw IllegalStateException(error.message), identity)
    require(
      derived.children
        .fold(error => throw IllegalStateException(error.message), identity)
        .map(_.path.value) == Vector("derived/mask"),
      s"$path derived children differ"
    )
    val mask = root
      .openArray("derived/mask")
      .fold(error => throw IllegalStateException(error.message), identity)
    readOpened(mask) match
      case PrimitiveBlock.UInt8(found) =>
        require(
          found.toArray.toVector == Vector[Byte](1, 1, 0, 0, 1, 1),
          s"$path mask values differ"
        )
      case found => throw IllegalStateException(s"expected uint8, found $found")

  private def assertFactored(parent: Path): Unit =
    val path = parent.resolve("python-factored.zarr")
    val expected =
      JsonParser.parse(Files.readString(parent.resolve("python-factored-expected.json"))) match
        case Right(JsonValue.Arr(values)) =>
          values.map:
            case JsonValue.Num(number) =>
              number.toLongExact match
                case Right(found) if found >= Int.MinValue.toLong && found <= Int.MaxValue.toLong =>
                  found.toInt
                case Right(found) =>
                  throw IllegalStateException(s"factored expected value $found is outside Int")
                case Left(detail) => throw IllegalStateException(detail)
            case found => throw IllegalStateException(s"expected number, found $found")
        case Right(found) => throw IllegalStateException(s"expected array, found $found")
        case Left(error)  => throw IllegalStateException(error.message)
    val store = JvmFileStore
      .open(path)
      .fold(detail => throw IllegalArgumentException(detail), identity)
    val opened = SyncZarr
      .openArray(store, runtime = JvmCodecRuntime.portable)
      .fold(error => throw IllegalStateException(error.message), identity)
    val selection = FactoredSelection
      .within(
        opened.descriptor.shape,
        Vector(
          AxisSelector.Indices(
            AxisIndices
              .from(Vector(6L, 1L, 6L, 0L))
              .fold(error => throw IllegalArgumentException(error.message), identity)
          ),
          AxisSelector.Slice(
            AxisSlice(1L, 8L, 3L)
              .fold(error => throw IllegalArgumentException(error.message), identity)
          ),
          AxisSelector.Indices(
            AxisIndices
              .from(Vector(8L, 2L, 8L, 0L))
              .fold(error => throw IllegalArgumentException(error.message), identity)
          )
        )
      )
      .fold(error => throw IllegalArgumentException(error.message), identity)
    val materialized = opened
      .read(selection)
      .fold(error => throw IllegalStateException(error.message), identity)
    materialized.block match
      case PrimitiveBlock.Int32(found) =>
        require(found.toArray.toVector == expected, s"$path materialized factored values differ")
      case found => throw IllegalStateException(s"expected int32, found $found")

    val streamed = opened.foldFragments(selection, Array.fill(expected.length)(Int.MinValue)):
      (output, fragment) =>
        val values = fragment.values match
          case PrimitiveBlock.Int32(found) => found.toArray
          case found => throw IllegalStateException(s"expected int32 fragment, found $found")
        val cursor = new Array[Long](fragment.shape.rank.toInt)
        var element = 0
        while element < values.length do
          var destination = 0L
          var axis = 0
          while axis < cursor.length do
            val placement = fragment.placement
              .axis(axis)
              .fold(error => throw IllegalStateException(error.message), identity)
            val outputIndex = placement
              .outputIndex(cursor(axis))
              .fold(error => throw IllegalStateException(error.message), identity)
            destination = destination * selection.outputShape.axis(axis) + outputIndex
            axis += 1
          output(destination.toInt) = values(element)
          advance(cursor, fragment.shape)
          element += 1
        Right(FragmentControl.Continue(output))
    val folded = streamed.fold(error => throw IllegalStateException(error.message), identity)
    require(folded.state.toVector == expected, s"$path streamed factored values differ")
    require(folded.receipt.completed, s"$path fragment stream did not complete")

  private def advance(cursor: Array[Long], shape: Shape): Unit =
    var axis = cursor.length - 1
    var advanced = false
    while axis >= 0 && !advanced do
      cursor(axis) += 1L
      if cursor(axis) < shape.axis(axis) then advanced = true
      else
        cursor(axis) = 0L
        axis -= 1

  private def read(path: Path): PrimitiveBlock =
    val store = JvmFileStore
      .open(path)
      .fold(detail => throw IllegalArgumentException(detail), identity)
    val opened = SyncZarr
      .openArray(store, runtime = JvmCodecRuntime.portable)
      .fold(error => throw IllegalStateException(error.message), identity)
    readOpened(opened)

  private def readOpened(opened: OpenedArray): PrimitiveBlock =
    val origin = Coordinate
      .from(Vector.fill(opened.descriptor.shape.rank.toInt)(0L))
      .fold(error => throw IllegalArgumentException(error.message), identity)
    val region = Region
      .within(opened.descriptor.shape, origin, opened.descriptor.shape)
      .fold(error => throw IllegalArgumentException(error.message), identity)
    val result = opened
      .readRegion(region)
      .fold(error => throw IllegalStateException(error.message), identity)
    result.block
