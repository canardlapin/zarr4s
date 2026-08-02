package zarr4s

import scala.concurrent.ExecutionContext.Implicits.global

class BrowserTypedReadSuite extends munit.FunSuite:
  private def bytes(value: String): OwnedBytes =
    OwnedBytes.copyOf(value.iterator.map(_.toByte).toArray)

  private val directBytesMetadata =
    ZarrBinaryFixtures.directGzipMetadata.replace(
      ",\"codecs\":[{\"configuration\":{\"endian\":\"little\"},\"name\":\"bytes\"},{\"configuration\":{\"level\":1},\"name\":\"gzip\"}]",
      ",\"codecs\":[{\"configuration\":{\"endian\":\"little\"},\"name\":\"bytes\"}]"
    )

  private def store: AsyncMemoryStore =
    AsyncMemoryStore(
      Map(
        "zarr.json" -> bytes(directBytesMetadata),
        "c/0/0" -> ZarrBinaryFixtures.directDecodedChunk
      )
    ) match
      case Right(found) => found
      case Left(error)  => throw IllegalStateException(error.message)

  private def value[A](result: Either[ZarrError, A]): A = result match
    case Right(found) => found
    case Left(error)  => throw IllegalStateException(error.message)

  test("BrowserZarr exposes the typed read-all facade"):
    val found = store
    BrowserZarr
      .openTypedArray(found, DType.Int16)
      .flatMap:
        case Left(error)   => fail(error.message)
        case Right(opened) =>
          opened
            .readAll()
            .map:
              case Left(error)   => fail(error.message)
              case Right(result) =>
                assertEquals(result.data.toArray.toVector, Vector[Short](1, -2, 300, 4, 5, -6))
                assertEquals(result.receipt.requestedElements, 6L)

  test("BrowserZarr typed mismatch is checked before data fetch"):
    val found = store
    BrowserZarr
      .openTypedArray(found, DType.Float32)
      .map:
        case Left(ZarrError.DTypeMismatch("float32", "int16", "opened array")) =>
          assertEquals(
            found.trace.collect { case ObjectRequest.Whole(key) => key.value },
            Vector("zarr.json")
          )
        case Left(error) => fail(error.message)
        case Right(_)    => fail("expected dtype mismatch")

  test("BrowserZarr create-and-open uses the portable async writer and reader"):
    val shape = value(Shape(2L, 2L))
    val chunks = value(Shape(1L, 2L))
    val arraySpec = value(ArraySpec(DType.Int16, shape, chunks))
    val data = value(DenseArray.copyOf(DType.Int16, shape, Array[Short](1, 2, 3, 4)))
    val target = value(AsyncMemoryStore(Map.empty))
    BrowserZarr
      .createAndOpenArray(target, arraySpec, data)
      .flatMap:
        case Left(error)    => fail(error.message)
        case Right(created) =>
          created.opened match
            case Left(error)   => fail(error.message)
            case Right(opened) =>
              opened
                .readAll()
                .map:
                  case Left(error)   => fail(error.message)
                  case Right(result) =>
                    assertEquals(result.data.toArray.toVector, Vector[Short](1, 2, 3, 4))
                    assertEquals(result.receipt.requestedElements, 4L)
