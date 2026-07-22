package scalafim.zarr

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/** Explicit bridge from blocking object IO to the portable asynchronous API.
  *
  * The supplied execution context must be dedicated to blocking IO; a compute
  * pool or a global execution context is not an appropriate boundary here.
  */
object BlockingObjectReaderAdapter:
  def apply(
      reader: ObjectReader,
      blockingExecutionContext: ExecutionContext
  ): AsyncObjectReader =
    new AsyncObjectReader:
      def read(
          key: StoreKey,
          range: ByteRange
      ): Future[Either[StoreError, OwnedBytes]] =
        Future(reader.read(key, range))(using blockingExecutionContext)

      def readAll(
          key: StoreKey,
          maxBytes: ByteCount
      ): Future[Either[StoreError, OwnedBytes]] =
        Future(reader.readAll(key, maxBytes))(using blockingExecutionContext)

      def length(key: StoreKey): Future[Either[StoreError, Long]] =
        Future(reader.length(key))(using blockingExecutionContext)
