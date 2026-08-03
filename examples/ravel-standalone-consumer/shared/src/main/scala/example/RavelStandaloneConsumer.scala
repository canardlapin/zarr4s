package example

import ravel.{NDArray, Shape as RavelShape}
import ravel.DType.given
import ravel.map
import zarr4s.*
import zarr4s.ravel.*

final case class ConsumerResult(
    values: Vector[Float],
    inputBytesRead: Long,
    outputObjects: Int
)

object RavelStandaloneConsumer:
  def run(): Either[String, ConsumerResult] =
    for
      shape <- zvalue(Shape(2L, 3L))
      chunks <- zvalue(Shape(1L, 3L))
      spec <- zvalue(ArraySpec(DType.Float32, shape, chunks))
      inputPath <- zvalue(ZarrPath("input"))
      outputPath <- zvalue(ZarrPath("output"))
      store <- zvalue(MemoryStore.empty)
      input = NDArray.fromSeq(
        RavelShape(2, 3),
        Seq(0.5f, 1.0f, 1.5f, 2.0f, 2.5f, 3.0f)
      )
      inputSource <- rvalue(RavelArraySource.fromCanonical(DType.Float32, input))
      created <- rvalue(
        RavelZarr.createAndOpenArray(store, spec, inputSource, path = inputPath)
      )
      _ <- zvalue(created.outcome.toEither)
      opened <- zvalue(created.opened)
      read <- rvalue(opened.readAllNDArray())
      transformed = read.data.map(value => value * 2.0f + 1.0f)
      outputSource <- rvalue(
        RavelArraySource.fromCanonical(DType.Float32, transformed)
      )
      written <- rvalue(RavelZarr.createArray(store, spec, outputSource, path = outputPath))
      receipt <- zvalue(written.outcome.toEither)
      output <- zvalue(SyncZarr.openTypedArray(store, DType.Float32, path = outputPath))
      verified <- rvalue(output.readAllNDArray())
    yield ConsumerResult(
      verified.data.elementsIterator.toVector,
      read.receipt.bytesRead,
      receipt.totalObjects
    )

  private def zvalue[A](result: Either[ZarrError, A]): Either[String, A] =
    result.left.map(_.message)

  private def rvalue[A](result: Either[RavelInteropError, A]): Either[String, A] =
    result.left.map(_.message)

object RavelStandaloneConsumerMain:
  def main(_arguments: Array[String]): Unit =
    RavelStandaloneConsumer.run() match
      case Right(result)
          if result.values == Vector(2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f) &&
            result.inputBytesRead == 24L && result.outputObjects == 3 =>
        println(result.values.map(_.toInt).mkString("transformed = [", ", ", "]"))
      case Right(result) =>
        throw new IllegalStateException(s"unexpected consumer result: $result")
      case Left(error) => throw new IllegalArgumentException(error)
