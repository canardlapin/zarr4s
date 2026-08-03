package zarr4s.ravel

import _root_.ravel.{AnyRank, NDArray, Shape as RavelShape, UInt8, UInt16}
import _root_.zarr4s.*
import java.nio.file.{Files, Path}

/** Filesystem court used by the pinned Zarr-Python and zarrs interoperability scripts. */
object RavelInteropFixtureMain:
  def main(arguments: Array[String]): Unit =
    require(arguments.length == 2, "expected mode and fixture parent")
    val parent = Path.of(arguments(1))
    arguments(0) match
      case "write-ravel"   => writeRavel(parent)
      case "verify-python" => verifyPython(parent)
      case mode            => throw new IllegalArgumentException(s"unsupported mode: $mode")

  private def writeRavel(parent: Path): Unit =
    Files.createDirectories(parent)
    val matrix = RavelShape(2, 3)
    write(
      parent,
      "ravel-bool.zarr",
      DType.Bool,
      NDArray.fromSeq(matrix, Seq(false, true, false, true, true, false))
    )
    write(
      parent,
      "ravel-int8.zarr",
      DType.Int8,
      NDArray.fromSeq(matrix, Seq[Byte](Byte.MinValue, -1, 0, 1, 42, Byte.MaxValue))
    )
    write(
      parent,
      "ravel-uint8.zarr",
      DType.UInt8,
      NDArray.fromSeq(matrix, Seq(0, 1, 127, 128, 254, 255).map(UInt8.unsafe))
    )
    write(
      parent,
      "ravel-int16.zarr",
      DType.Int16,
      NDArray.fromSeq(matrix, Seq[Short](Short.MinValue, -1, 0, 1, 42, Short.MaxValue))
    )
    write(
      parent,
      "ravel-uint16.zarr",
      DType.UInt16,
      NDArray.fromSeq(matrix, Seq(0, 1, 32767, 32768, 65534, 65535).map(UInt16.unsafe))
    )
    write(
      parent,
      "ravel-int32.zarr",
      DType.Int32,
      NDArray.fromSeq(matrix, Seq(Int.MinValue, -1, 0, 1, 42, Int.MaxValue))
    )
    write(
      parent,
      "ravel-int64.zarr",
      DType.Int64,
      NDArray.fromSeq(matrix, Seq(Long.MinValue, -1L, 0L, 1L, 42L, Long.MaxValue))
    )
    write(
      parent,
      "ravel-float32.zarr",
      DType.Float32,
      NDArray.fromSeq(matrix, Seq(0.0f, -0.0f, 1.5f, -2.25f, Float.PositiveInfinity, Float.NaN))
    )
    write(
      parent,
      "ravel-float64.zarr",
      DType.Float64,
      NDArray.fromSeq(matrix, Seq(0.0, -0.0, 1.5, -2.25, Double.PositiveInfinity, Double.NaN))
    )

    write(
      parent,
      "ravel-border.zarr",
      DType.Int32,
      NDArray.fromSeq(RavelShape(3, 5), 0 until 15),
      chunks = Shape.unsafe(Array(2L, 3L))
    )
    write(
      parent,
      "ravel-sharded.zarr",
      DType.Int16,
      NDArray.fromSeq(RavelShape(4, 4), (1 to 16).map(_.toShort)),
      chunks = Shape.unsafe(Array(4L, 4L)),
      sharding = Some(ShardingSpec.indexed(Shape.unsafe(Array(2L, 2L))))
    )
    write(
      parent,
      "ravel-v2-int16.zarr",
      DType.Int16,
      NDArray.fromSeq(matrix, Seq[Short](1, -2, 300, 4, 5, -6)),
      format = ZarrFormat.V2
    )

  private def verifyPython(parent: Path): Unit =
    verify(parent, "python-bool.zarr", DType.Bool, Vector(false, true, false, true, true, false))
    verify(
      parent,
      "python-int8.zarr",
      DType.Int8,
      Vector[Byte](Byte.MinValue, -1, 0, 1, 42, Byte.MaxValue)
    )
    verify(
      parent,
      "python-uint8.zarr",
      DType.UInt8,
      Vector(0, 1, 127, 128, 254, 255).map(UInt8.unsafe)
    )
    verify(
      parent,
      "python-int16.zarr",
      DType.Int16,
      Vector[Short](Short.MinValue, -1, 0, 1, 42, Short.MaxValue)
    )
    verify(
      parent,
      "python-uint16.zarr",
      DType.UInt16,
      Vector(0, 1, 32767, 32768, 65534, 65535).map(UInt16.unsafe)
    )
    verify(
      parent,
      "python-int32.zarr",
      DType.Int32,
      Vector(Int.MinValue, -1, 0, 1, 42, Int.MaxValue)
    )
    verify(
      parent,
      "python-int64.zarr",
      DType.Int64,
      Vector(Long.MinValue, -1L, 0L, 1L, 42L, Long.MaxValue)
    )
    verify(
      parent,
      "python-float32.zarr",
      DType.Float32,
      Vector(0.0f, -0.0f, 1.5f, -2.25f, Float.PositiveInfinity, Float.NegativeInfinity)
    )
    verify(
      parent,
      "python-float64.zarr",
      DType.Float64,
      Vector(0.0, -0.0, 1.5, -2.25, Double.PositiveInfinity, Double.NegativeInfinity)
    )

  private def write[D <: DType & Singleton, A, R <: AnyRank](
      parent: Path,
      name: String,
      dtype: D,
      array: NDArray[A, R],
      chunks: Shape = Shape.unsafe(Array(2L, 3L)),
      format: ZarrFormat = ZarrFormat.V3,
      sharding: Option[ShardingSpec] = None
  )(using mapping: RavelElement[D], elementType: A =:= RavelValue[D]): Unit =
    val source = rvalue(
      RavelArraySource.fromCanonical[D, A, R](dtype, array)(using mapping, elementType)
    )
    val target = parent.resolve(name)
    Files.createDirectory(target)
    val store = zvalue(JvmFileStore.openChecked(target))
    val spec = zvalue(ArraySpec(dtype, source.shape, chunks)).asFormat(format)
    rvalue(RavelZarr.createArray(store, spec, source, sharding = sharding)).outcome match
      case WriteOutcome.Complete(_)          => ()
      case WriteOutcome.Incomplete(_, error) => throw new IllegalStateException(error.message)

  private def verify[D <: DType & Singleton](
      parent: Path,
      name: String,
      dtype: D,
      expected: Vector[RavelValue[D]]
  )(using RavelElement[D]): Unit =
    val opened = zvalue(JvmZarr.openTypedArray(parent.resolve(name), dtype))
    val result = rvalue(opened.readAllNDArray())
    val found = result.data.elementsIterator.toVector
    if found != expected then
      throw new IllegalStateException(s"$name values differ: $found != $expected")

  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(value) => value
    case Left(error)  => throw new IllegalStateException(error.message)

  private def rvalue[A](result: Either[RavelInteropError, A]): A = result match
    case Right(value) => value
    case Left(error)  => throw new IllegalStateException(error.message)
