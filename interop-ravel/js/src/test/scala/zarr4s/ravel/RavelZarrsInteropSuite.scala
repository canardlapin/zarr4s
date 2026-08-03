package zarr4s.ravel

import _root_.zarr4s.*
import scala.concurrent.ExecutionContext.Implicits.global

class RavelZarrsInteropSuite extends munit.FunSuite:
  test("Scala.js materializes the exact independent zarrs shard as a Ravel UInt16 array"):
    val store = zvalue(AsyncMemoryStore(ZarrsFixtures.objects))
    AsyncZarr
      .openTypedArray(store, DType.UInt16, runtime = BrowserCodecRuntime.portable)
      .flatMap:
        case Left(error)   => fail(error.message)
        case Right(opened) => opened.readAllNDArrayAsync()
      .map:
        case Left(error)   => fail(error.message)
        case Right(result) =>
          assertEquals(result.data.elementsIterator.map(_.toInt).toVector, (0 until 64).toVector)
          assertEquals(result.receipt.touchedShards, 2)
          assertEquals(
            ZarrsFixtures.sourceCommit,
            "cf8209811f5937cbe4594a7a3445b95c9d35872c"
          )

  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(value) => value
    case Left(error)  => fail(error.message)
