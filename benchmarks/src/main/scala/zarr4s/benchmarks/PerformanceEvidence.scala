package zarr4s.benchmarks

import java.util.Locale
import zarr4s.*

final case class EvidenceRow(
    id: String,
    layout: String,
    workload: String,
    phase: String,
    logicalBytes: Long,
    bytesRead: Long,
    indexBytesRead: Long,
    dataBytesRead: Long,
    objectRequests: Int,
    rangeRequests: Int,
    lengthRequests: Int,
    downstreamRequests: Long,
    fetchedBytes: Long,
    cacheHits: Long,
    touchedChunks: Int,
    touchedShards: Int,
    readAmplification: Double
)

object PerformanceEvidence:
  private final case class Layout(name: String, store: MemoryStore)
  private final case class Workload(name: String, region: Region)
  private final case class Measured(row: EvidenceRow, values: Vector[Short])

  val csvHeader: String =
    "id,layout,workload,phase,logical_bytes,bytes_read,index_bytes_read,data_bytes_read," +
      "object_requests,range_requests,length_requests,downstream_requests,fetched_bytes," +
      "cache_hits,touched_chunks,touched_shards,read_amplification"

  def rows: Either[String, Vector[EvidenceRow]] =
    for
      shape <- checked(Shape(32L, 32L, 16L, 64L))
      balanced <- checked(Shape(16L, 16L, 8L, 8L))
      volumeOriented <- checked(Shape(32L, 32L, 16L, 1L))
      shardShape <- checked(Shape(32L, 32L, 16L, 16L))
      elementCount <- checked(shape.elementCount)
      data <- checked(
        DenseArray.copyOf(
          DType.Int16,
          shape,
          Array.tabulate(elementCount.toInt)(index => ((index * 17) % 1024).toShort)
        )
      )
      layouts <- sequence(
        Vector(
          direct("direct-balanced", shape, balanced, data),
          direct("direct-volume", shape, volumeOriented, data),
          sharded("sharded-balanced", shape, shardShape, balanced, data)
        )
      )
      workloads <- workloadMatrix(shape)
      cold <- traverse(layouts): layout =>
        traverse(workloads): workload =>
          measure(layout, workload, includeWarm = false).map(_.head)
      warm <- traverse(layouts.filter(layout => layout.name != "direct-volume")): layout =>
        measure(layout, workloads.head, includeWarm = true).map(_.last)
      measured = cold.flatten ++ warm
      _ <- verifyParity(measured)
    yield measured.map(_.row)

  def csv: Either[String, String] = rows.map(renderCsv)

  def chunkGuideTable: Either[String, String] = rows.map: values =>
    markdownTable(
      Vector("Layout", "Workload", "Object requests", "Bytes read", "Amplification"),
      values
        .filter(row => row.phase == "cold" && row.layout.startsWith("direct-"))
        .map: row =>
          Vector(
            row.layout,
            row.workload,
            row.objectRequests.toString,
            row.bytesRead.toString,
            formatAmplification(row.readAmplification)
          )
    )

  def shardGuideTable: Either[String, String] = rows.map: values =>
    markdownTable(
      Vector(
        "Layout",
        "Workload",
        "Object requests",
        "Range requests",
        "Index bytes",
        "Total bytes",
        "Amplification"
      ),
      values
        .filter: row =>
          row.phase == "cold" &&
            (row.layout == "direct-balanced" || row.layout == "sharded-balanced")
        .map: row =>
          Vector(
            row.layout,
            row.workload,
            row.objectRequests.toString,
            row.rangeRequests.toString,
            row.indexBytesRead.toString,
            row.bytesRead.toString,
            formatAmplification(row.readAmplification)
          )
    )

  def cacheGuideTable: Either[String, String] = rows.map: values =>
    markdownTable(
      Vector("Layout", "Phase", "Downstream requests", "Fetched bytes", "Cache hits"),
      values
        .filter: row =>
          row.workload == "volume" &&
            (row.layout == "direct-balanced" || row.layout == "sharded-balanced")
        .map: row =>
          Vector(
            row.layout,
            row.phase,
            row.downstreamRequests.toString,
            row.fetchedBytes.toString,
            row.cacheHits.toString
          )
    )

  def renderCsv(values: Vector[EvidenceRow]): String =
    (csvHeader +: values.map(renderRow)).mkString("\n") + "\n"

  def main(_args: Array[String]): Unit =
    csv match
      case Left(error)  => throw new IllegalStateException(error)
      case Right(value) => print(value)

  private def direct(
      name: String,
      shape: Shape,
      chunkShape: Shape,
      data: DenseArray[DType.Int16.type]
  ): Either[String, Layout] =
    for
      store <- checked(MemoryStore.empty)
      spec <- checked(ArraySpec(DType.Int16, shape, chunkShape))
      result <- checked(SyncZarr.createArray(store, spec, data))
      _ <- checked(result.outcome.toEither)
    yield Layout(name, store)

  private def sharded(
      name: String,
      shape: Shape,
      shardShape: Shape,
      innerChunkShape: Shape,
      data: DenseArray[DType.Int16.type]
  ): Either[String, Layout] =
    for
      store <- checked(MemoryStore.empty)
      spec <- checked(ArraySpec(DType.Int16, shape, shardShape))
      result <- checked(
        SyncZarr.createArray(
          store,
          spec,
          data,
          sharding = Some(ShardingSpec.indexed(innerChunkShape))
        )
      )
      _ <- checked(result.outcome.toEither)
    yield Layout(name, store)

  private def workloadMatrix(shape: Shape): Either[String, Vector[Workload]] =
    sequence(
      Vector(
        workload("volume", shape, Vector(0L, 0L, 0L, 8L), Vector(32L, 32L, 16L, 1L)),
        workload("movie-16", shape, Vector(0L, 0L, 0L, 16L), Vector(32L, 32L, 16L, 16L)),
        workload("aligned-roi", shape, Vector(16L, 16L, 8L, 16L), Vector(16L, 16L, 8L, 8L)),
        workload("voxel-series", shape, Vector(10L, 10L, 5L, 0L), Vector(1L, 1L, 1L, 64L))
      )
    )

  private def workload(
      name: String,
      shape: Shape,
      origin: Vector[Long],
      extent: Vector[Long]
  ): Either[String, Workload] =
    for
      checkedOrigin <- checked(Coordinate.from(origin))
      checkedExtent <- checked(Shape.from(extent))
      region <- checked(Region.within(shape, checkedOrigin, checkedExtent))
    yield Workload(name, region)

  private def measure(
      layout: Layout,
      workload: Workload,
      includeWarm: Boolean
  ): Either[String, Vector[Measured]] =
    for
      namespace <- checked(CacheNamespace.from(s"evidence-${layout.name}-${workload.name}"))
      cacheBytes <- checked(ByteCount(8L * 1024L * 1024L))
      cache = ObjectReadCache(namespace, CacheLimits(256, cacheBytes))
      opened <- checked(
        SyncZarr.openTypedArray(CachingObjectReader(layout.store, cache), DType.Int16)
      )
      before = cache.stats
      cold <- checked(opened.readRegion(workload.region))
      afterCold = cache.stats
      coldMeasured = measured(layout, workload, "cold", cold, before, afterCold)
      result <-
        if includeWarm then
          for
            warm <- checked(opened.readRegion(workload.region))
            afterWarm = cache.stats
            _ <-
              if cold.data.toArray.toVector == warm.data.toArray.toVector then Right(())
              else Left(s"warm cache changed values for ${layout.name}/${workload.name}")
          yield Vector(
            coldMeasured,
            measured(layout, workload, "warm", warm, afterCold, afterWarm)
          )
        else Right(Vector(coldMeasured))
    yield result

  private def measured(
      layout: Layout,
      workload: Workload,
      phase: String,
      read: TypedReadResult[DType.Int16.type],
      before: CacheStats,
      after: CacheStats
  ): Measured =
    val receipt = read.receipt
    Measured(
      EvidenceRow(
        id = s"${layout.name}.${workload.name}.$phase",
        layout = layout.name,
        workload = workload.name,
        phase = phase,
        logicalBytes = receipt.requestedLogicalBytes,
        bytesRead = receipt.bytesRead,
        indexBytesRead = receipt.indexBytesRead,
        dataBytesRead = receipt.dataBytesRead,
        objectRequests = receipt.objectRequests,
        rangeRequests = receipt.rangeRequests,
        lengthRequests = receipt.lengthRequests,
        downstreamRequests = after.downstreamRequests - before.downstreamRequests,
        fetchedBytes = after.fetchedBytes - before.fetchedBytes,
        cacheHits = after.hits - before.hits,
        touchedChunks = receipt.touchedChunks,
        touchedShards = receipt.touchedShards,
        readAmplification = receipt.readAmplification
      ),
      read.data.toArray.toVector
    )

  private def verifyParity(values: Vector[Measured]): Either[String, Unit] =
    val cold = values.filter(_.row.phase == "cold").groupBy(_.row.workload)
    cold.iterator
      .find((_, rows) => rows.map(_.values).distinct.length != 1)
      .map((workload, _) => Left(s"layouts returned different values for $workload"))
      .getOrElse(Right(()))

  private def renderRow(row: EvidenceRow): String =
    Vector(
      row.id,
      row.layout,
      row.workload,
      row.phase,
      row.logicalBytes.toString,
      row.bytesRead.toString,
      row.indexBytesRead.toString,
      row.dataBytesRead.toString,
      row.objectRequests.toString,
      row.rangeRequests.toString,
      row.lengthRequests.toString,
      row.downstreamRequests.toString,
      row.fetchedBytes.toString,
      row.cacheHits.toString,
      row.touchedChunks.toString,
      row.touchedShards.toString,
      formatAmplification(row.readAmplification)
    ).mkString(",")

  private def formatAmplification(value: Double): String =
    String.format(Locale.ROOT, "%.6f", Double.box(value))

  private def markdownTable(headers: Vector[String], values: Vector[Vector[String]]): String =
    val header = headers.mkString("| ", " | ", " |")
    val separator = headers.map(_ => "---").mkString("| ", " | ", " |")
    val rows = values.map(_.mkString("| ", " | ", " |"))
    (header +: separator +: rows).mkString("\n")

  private def checked[A](value: Either[ZarrError, A]): Either[String, A] =
    value.left.map(_.message)

  private def sequence[A](values: Vector[Either[String, A]]): Either[String, Vector[A]] =
    traverse(values)(identity)

  private def traverse[A, B](values: Vector[A])(
      use: A => Either[String, B]
  ): Either[String, Vector[B]] =
    values.foldLeft[Either[String, Vector[B]]](Right(Vector.empty)): (found, value) =>
      found.flatMap(accumulated => use(value).map(accumulated :+ _))
