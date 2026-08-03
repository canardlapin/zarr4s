package zarr4s.ravel

import _root_.zarr4s.*

class RavelZarrsInteropSuite extends munit.FunSuite:
  test("JVM materializes the exact independent zarrs shard as a Ravel UInt16 array"):
    val store = zvalue(MemoryStore(ZarrsFixtures.objects))
    val opened = zvalue(
      SyncZarr.openTypedArray(store, DType.UInt16, runtime = JvmCodecRuntime.portable)
    )
    val result = rvalue(opened.readAllNDArray())

    assertEquals(result.data.elementsIterator.map(_.toInt).toVector, (0 until 64).toVector)
    assertEquals(result.receipt.touchedShards, 2)
    assertEquals(ZarrsFixtures.sourceCommit, "cf8209811f5937cbe4594a7a3445b95c9d35872c")

  private def zvalue[A](result: Either[ZarrError, A]): A = result match
    case Right(value) => value
    case Left(error)  => fail(error.message)

  private def rvalue[A](result: Either[RavelInteropError, A]): A = result match
    case Right(value) => value
    case Left(error)  => fail(error.message)
