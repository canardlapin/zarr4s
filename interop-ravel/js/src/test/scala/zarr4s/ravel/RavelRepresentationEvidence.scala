package zarr4s.ravel

import _root_.ravel.{NDArray, Shape as RavelShape}
import _root_.zarr4s.*
import scala.scalajs.js

final case class RavelJsRepresentationRow(
    scenario: String,
    logicalBytes: Long,
    chunkBytes: Long,
    arrayBufferDeltaBytes: Long,
    elapsedMilliseconds: Double,
    canonical: Boolean,
    wholeBuffer: Boolean,
    exactDType: Boolean
)

object RavelRepresentationEvidence:
  private val side = 512
  private val elementCount = side * side
  private val logicalBytes = elementCount.toLong * Integer.BYTES
  private val chunkSide = 64
  private val chunkBytes = chunkSide.toLong * chunkSide.toLong * Integer.BYTES
  private val zarrShape = Shape.unsafe(Array(side.toLong, side.toLong))
  private val ravelShape = RavelShape(side, side)
  private val values = Array.tabulate(elementCount)(index => index * 31)
  private val block = PrimitiveBlock.Int32(OwnedInts.copyOf(values))
  private val canonical = NDArray.fromSeq(ravelShape, values.toIndexedSeq)

  val csvHeader =
    "scenario,logical_bytes,chunk_bytes,array_buffer_delta_bytes,elapsed_milliseconds," +
      "canonical,whole_buffer,exact_dtype"

  def rows: Vector[RavelJsRepresentationRow] =
    val retained = new js.Array[Any]()
    val direct = measured(retained, "direct-read-materialize", logicalBytes, 0L): () =>
      rvalue(summon[RavelElement[DType.Int32.type]].materialize(block, zarrShape))
    val dense = measured(retained, "dense-bridge-materialize", logicalBytes, 0L): () =>
      val dense = zvalue(DenseArray.copyOf(DType.Int32, zarrShape, blockValues.toArray))
      NDArray.fromSeq(ravelShape, dense.toArray.toIndexedSeq)
    val source = rvalue(RavelArraySource.fromCanonical(DType.Int32, canonical))
    val descriptor = zvalue(
      ArrayDescriptor.direct(
        zvalue(
          ArraySpec(
            DType.Int32,
            zarrShape,
            Shape.unsafe(Array(chunkSide.toLong, chunkSide.toLong))
          )
        )
      )
    )
    val provider = rvalue(source.typedProvider(descriptor)).underlying
    val canonicalSource = measured(retained, "canonical-write-source", logicalBytes, 0L): () =>
      rvalue(RavelArraySource.fromCanonical(DType.Int32, canonical)).array
    val chunk = measuredBlock(retained, "canonical-write-chunk", logicalBytes, chunkBytes): () =>
      zvalue(
        provider.chunk(
          ChunkCoordinate.unsafe(Array(3L, 4L)),
          descriptor.grid.chunkShape
        )
      )
    val copied = measured(retained, "explicit-view-materialization", logicalBytes, 0L): () =>
      rvalue(RavelArraySource.copyOf(DType.Int32, canonical.reverse(1))).array
    Vector(direct, dense, canonicalSource, chunk, copied)

  def csv: String =
    (csvHeader +: rows.map(render)).mkString("\n") + "\n"

  def main(_arguments: Array[String]): Unit = print(csv)

  private def measured(
      retained: js.Array[Any],
      scenario: String,
      scenarioLogicalBytes: Long,
      scenarioChunkBytes: Long
  )(operation: () => _root_.ravel.NDArray[Int, ?]): RavelJsRepresentationRow =
    val before = arrayBufferBytes()
    val started = js.Date.now()
    val result = operation()
    retained.push(result)
    val elapsed = js.Date.now() - started
    val after = arrayBufferBytes()
    RavelJsRepresentationRow(
      scenario,
      scenarioLogicalBytes,
      scenarioChunkBytes,
      math.max(0L, after - before),
      elapsed,
      result.isCanonicalLayout,
      result.isWholeBuffer,
      result.dtype == summon[_root_.ravel.DType[Int]]
    )

  private def measuredBlock(
      retained: js.Array[Any],
      scenario: String,
      scenarioLogicalBytes: Long,
      scenarioChunkBytes: Long
  )(operation: () => ChunkPayload): RavelJsRepresentationRow =
    val before = arrayBufferBytes()
    val started = js.Date.now()
    val payload = operation()
    retained.push(payload)
    val elapsed = js.Date.now() - started
    val after = arrayBufferBytes()
    val exact = payload match
      case ChunkPayload.Values(_: PrimitiveBlock.Int32) => true
      case _                                            => false
    RavelJsRepresentationRow(
      scenario,
      scenarioLogicalBytes,
      scenarioChunkBytes,
      math.max(0L, after - before),
      elapsed,
      canonical = true,
      wholeBuffer = true,
      exactDType = exact
    )

  private def arrayBufferBytes(): Long =
    js.Dynamic.global.process.memoryUsage().arrayBuffers.asInstanceOf[Double].toLong

  private def blockValues: OwnedInts = block match
    case PrimitiveBlock.Int32(found) => found
    case _                           => throw new IllegalStateException("expected int32 block")

  private def render(row: RavelJsRepresentationRow): String =
    Vector(
      row.scenario,
      row.logicalBytes.toString,
      row.chunkBytes.toString,
      row.arrayBufferDeltaBytes.toString,
      row.elapsedMilliseconds.toString,
      row.canonical.toString,
      row.wholeBuffer.toString,
      row.exactDType.toString
    ).mkString(",")

  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(value) => value
    case Left(error)  => throw new IllegalStateException(error.message)

  private def rvalue[A](result: Either[RavelInteropError, A]): A = result match
    case Right(value) => value
    case Left(error)  => throw new IllegalStateException(error.message)
