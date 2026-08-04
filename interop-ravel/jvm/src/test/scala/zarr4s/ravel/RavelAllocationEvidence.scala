package zarr4s.ravel

import _root_.ravel.{NDArray, Shape as RavelShape}
import _root_.zarr4s.*
import com.sun.management.{ThreadMXBean as SunThreadMXBean}
import java.lang.management.ManagementFactory
import java.util.Locale

final case class RavelAllocationRow(
    scenario: String,
    logicalBytes: Long,
    chunkBytes: Long,
    warmupIterations: Int,
    measurementIterations: Int,
    allocatedBytesPerOperation: Long,
    peakHeapDeltaBytes: Long,
    nanosecondsPerOperation: Long,
    throughputMiBPerSecond: Double,
    checksum: Int
)

object RavelAllocationEvidence:
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
    "scenario,logical_bytes,chunk_bytes,warmup_iterations,measurement_iterations," +
      "allocated_bytes_per_operation,peak_heap_delta_bytes,nanoseconds_per_operation," +
      "throughput_mib_per_second,checksum"

  def rows: Vector[RavelAllocationRow] =
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
    val coordinate = ChunkCoordinate.unsafe(Array(3L, 4L))
    val view = canonical.reverse(1)

    Vector(
      measure("direct-read-materialize", logicalBytes, 0L, 5, 20): () =>
        directMaterialize().size,
      measure("dense-bridge-materialize", logicalBytes, 0L, 2, 8): () =>
        denseBridge().size,
      measure("canonical-write-source", logicalBytes, 0L, 100, 1000): () =>
        rvalue(RavelArraySource.fromCanonical(DType.Int32, canonical)).elementCount,
      measure("canonical-write-chunk", logicalBytes, chunkBytes, 20, 200): () =>
        zvalue(provider.chunk(coordinate, descriptor.grid.chunkShape)) match
          case ChunkPayload.Fill          => 0
          case ChunkPayload.Values(found) => found.elementCount,
      measure("explicit-view-materialization", logicalBytes, 0L, 2, 8): () =>
        rvalue(RavelArraySource.copyOf(DType.Int32, view)).elementCount
    )

  def correctness: Either[String, Unit] =
    val direct = directMaterialize()
    val dense = denseBridge()
    val source = rvalue(RavelArraySource.fromCanonical(DType.Int32, canonical))
    val copied = rvalue(RavelArraySource.copyOf(DType.Int32, canonical.reverse(1)))
    if !direct.sameElementsBits(canonical) then Left("direct materialization changed values")
    else if !dense.sameElementsBits(canonical) then Left("DenseArray bridge changed values")
    else if !(source.array.asInstanceOf[AnyRef] eq canonical.asInstanceOf[AnyRef]) then
      Left("canonical source refinement changed the Ravel owner")
    else if copied.array.asInstanceOf[AnyRef] eq canonical.asInstanceOf[AnyRef] then
      Left("explicit view materialization retained the original owner")
    else Right(())

  def csv: String =
    (csvHeader +: rows.map(render)).mkString("\n") + "\n"

  def main(_arguments: Array[String]): Unit =
    correctness.fold(error => throw new IllegalStateException(error), identity)
    print(csv)

  private def directMaterialize(): _root_.ravel.AnyNDArray[Int] =
    rvalue(summon[RavelElement[DType.Int32.type]].materialize(block, zarrShape))

  private def denseBridge(): _root_.ravel.AnyNDArray[Int] =
    val dense = zvalue(DenseArray.copyOf(DType.Int32, zarrShape, blockValues.toArray))
    NDArray.fromSeq(ravelShape, dense.toArray.toIndexedSeq)

  private def blockValues: OwnedInts = block match
    case PrimitiveBlock.Int32(found) => found
    case _                           => throw new IllegalStateException("expected int32 block")

  private def measure(
      scenario: String,
      scenarioLogicalBytes: Long,
      scenarioChunkBytes: Long,
      warmupIterations: Int,
      measurementIterations: Int
  )(operation: () => Int): RavelAllocationRow =
    var warmup = 0
    var checksum = 0
    while warmup < warmupIterations do
      checksum ^= operation()
      warmup += 1

    val bean = ManagementFactory.getThreadMXBean.asInstanceOf[SunThreadMXBean]
    if !bean.isThreadAllocatedMemoryEnabled then bean.setThreadAllocatedMemoryEnabled(true)
    val thread = Thread.currentThread().getId
    val beforeAllocated = bean.getThreadAllocatedBytes(thread)
    val started = System.nanoTime()
    var iteration = 0
    while iteration < measurementIterations do
      checksum ^= operation()
      iteration += 1
    val elapsed = System.nanoTime() - started
    val allocated = bean.getThreadAllocatedBytes(thread) - beforeAllocated

    System.gc()
    val pools = ManagementFactory.getMemoryPoolMXBeans
    val iterator = pools.iterator()
    var baseline = 0L
    while iterator.hasNext do
      val pool = iterator.next()
      if pool.getType == java.lang.management.MemoryType.HEAP then
        baseline += pool.getUsage.getUsed
        pool.resetPeakUsage()
    checksum ^= operation()
    val peakIterator = pools.iterator()
    var peak = 0L
    while peakIterator.hasNext do
      val pool = peakIterator.next()
      if pool.getType == java.lang.management.MemoryType.HEAP then peak += pool.getPeakUsage.getUsed

    val nanosPerOperation = elapsed / measurementIterations.toLong
    val seconds = elapsed.toDouble / 1_000_000_000.0
    val throughput =
      scenarioLogicalBytes.toDouble * measurementIterations.toDouble / seconds / (1024.0 * 1024.0)
    RavelAllocationRow(
      scenario,
      scenarioLogicalBytes,
      scenarioChunkBytes,
      warmupIterations,
      measurementIterations,
      allocated / measurementIterations.toLong,
      math.max(0L, peak - baseline),
      nanosPerOperation,
      throughput,
      checksum
    )

  private def render(row: RavelAllocationRow): String =
    Vector(
      row.scenario,
      row.logicalBytes.toString,
      row.chunkBytes.toString,
      row.warmupIterations.toString,
      row.measurementIterations.toString,
      row.allocatedBytesPerOperation.toString,
      row.peakHeapDeltaBytes.toString,
      row.nanosecondsPerOperation.toString,
      String.format(Locale.ROOT, "%.3f", Double.box(row.throughputMiBPerSecond)),
      row.checksum.toString
    ).mkString(",")

  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(value) => value
    case Left(error)  => throw new IllegalStateException(error.message)

  private def rvalue[A](result: Either[RavelInteropError, A]): A = result match
    case Right(value) => value
    case Left(error)  => throw new IllegalStateException(error.message)
