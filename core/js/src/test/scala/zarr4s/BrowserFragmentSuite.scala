package zarr4s

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.Promise

class BrowserFragmentSuite extends munit.FunSuite:
  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => fail(error.message)

  private def bytes(value: String): OwnedBytes =
    OwnedBytes.copyOf(value.iterator.map(_.toByte).toArray)

  private def assemble(fragments: Vector[ChunkFragment], shape: Shape): Vector[Short] =
    val output = Array.fill(zvalue(shape.elementCount).toInt)(Short.MinValue)
    fragments.foreach: fragment =>
      val values = fragment.values match
        case PrimitiveBlock.Int16(found) => found.toArray
        case _                           => fail("expected int16 fragment")
      val cursor = new Array[Long](fragment.shape.rank.toInt)
      var element = 0
      while element < values.length do
        var destination = 0L
        var axis = 0
        while axis < cursor.length do
          val outputIndex = zvalue(zvalue(fragment.placement.axis(axis)).outputIndex(cursor(axis)))
          destination = destination * shape.axis(axis) + outputIndex
          axis += 1
        output(destination.toInt) = values(element)
        advance(cursor, fragment.shape)
        element += 1
    output.toVector

  private def advance(cursor: Array[Long], shape: Shape): Unit =
    var axis = cursor.length - 1
    var advanced = false
    while axis >= 0 && !advanced do
      cursor(axis) += 1L
      if cursor(axis) < shape.axis(axis) then advanced = true
      else
        cursor(axis) = 0L
        axis -= 1

  test("browser fold emits compact factored fragments"):
    val store = zvalue(
      AsyncMemoryStore(
        Map(
          "zarr.json" -> bytes(ZarrBinaryFixtures.directGzipMetadata),
          "c/0/0" -> ZarrBinaryFixtures.directGzipChunk
        )
      )
    )
    BrowserZarr
      .openArray(store)
      .flatMap:
        case Left(error)   => fail(error.message)
        case Right(opened) =>
          val selected = zvalue(
            FactoredSelection.within(
              opened.descriptor.shape,
              Vector(
                AxisSelector.Indices(zvalue(AxisIndices.from(Vector(1L, 0L, 1L)))),
                AxisSelector.Slice(zvalue(AxisSlice(0L, 3L, 2L)))
              )
            )
          )
          opened
            .foldFragments(selected, Vector.empty[ChunkFragment])((fragments, fragment) =>
              Future.successful(Right(FragmentControl.Continue(fragments :+ fragment)))
            )
            .map:
              case Left(error)   => fail(error.message)
              case Right(folded) =>
                assertEquals(
                  assemble(folded.state, selected.outputShape),
                  Vector[Short](
                    4, -6, 1, 300, 4, -6
                  )
                )
                assertEquals(folded.receipt.decodedChunks, 1)
                assertEquals(folded.receipt.emittedElements, 6L)
                assert(folded.receipt.completed)

  test("browser sharded fold preserves bounded backpressure and early stop"):
    val store = zvalue(
      AsyncMemoryStore(
        Map(
          "zarr.json" -> bytes(ZarrBinaryFixtures.shardedStartMetadata),
          "c/0/0" -> ZarrBinaryFixtures.shardedStartObject
        )
      )
    )
    BrowserZarr
      .openArray(store)
      .flatMap:
        case Left(error)   => fail(error.message)
        case Right(opened) =>
          val selected = FactoredSelection.all(opened.descriptor.shape)
          val entered = Promise[Unit]()
          val release = Promise[Either[ZarrError, FragmentControl[Int]]]()
          store.clearTrace()
          val folded = opened.foldFragments(selected, 0): (_, _) =>
            entered.trySuccess(())
            release.future
          entered.future.flatMap: _ =>
            assertEquals(store.trace.length, 2)
            release.success(Right(FragmentControl.Stop(1)))
            folded.map:
              case Left(error)  => fail(error.message)
              case Right(found) =>
                assertEquals(found.state, 1)
                assertEquals(found.receipt.visitedChunks, 1)
                assert(!found.receipt.completed)
                assertEquals(store.trace.length, 2)

  test("browser sharded fold emits decoded and fill fragments"):
    val store = zvalue(
      AsyncMemoryStore(
        Map(
          "zarr.json" -> bytes(ZarrBinaryFixtures.shardedStartMetadata),
          "c/0/0" -> ZarrBinaryFixtures.shardedStartObject
        )
      )
    )
    BrowserZarr
      .openArray(store)
      .flatMap:
        case Left(error)   => fail(error.message)
        case Right(opened) =>
          val selected = FactoredSelection.all(opened.descriptor.shape)
          store.clearTrace()
          opened
            .foldFragments(selected, Vector.empty[ChunkFragment])((fragments, fragment) =>
              Future.successful(Right(FragmentControl.Continue(fragments :+ fragment)))
            )
            .map:
              case Left(error)   => fail(error.message)
              case Right(folded) =>
                assertEquals(
                  assemble(folded.state, selected.outputShape),
                  Vector[Short](
                    1, 2, 0, 0, 3, 4, 0, 0, 0, 0, 13, 14, 0, 0, 15, 16
                  )
                )
                assertEquals(folded.receipt.decodedChunks, 2)
                assertEquals(folded.receipt.fillChunks, 2)
                assertEquals(folded.receipt.rangeRequests, 3)
                assert(folded.receipt.completed)

  test("browser fragment fold propagates decode failure before callback"):
    val store = zvalue(
      AsyncMemoryStore(
        Map(
          "zarr.json" -> bytes(ZarrBinaryFixtures.directGzipMetadata),
          "c/0/0" -> OwnedBytes.copyOf(Array[Byte](1, 2, 3))
        )
      )
    )
    BrowserZarr
      .openArray(store)
      .flatMap:
        case Left(error)   => fail(error.message)
        case Right(opened) =>
          var calls = 0
          opened
            .foldFragments(FactoredSelection.all(opened.descriptor.shape), ()): (_, _) =>
              calls += 1
              Future.successful(Right(FragmentControl.Continue(())))
            .map: result =>
              assert(result.isLeft)
              assertEquals(calls, 0)
