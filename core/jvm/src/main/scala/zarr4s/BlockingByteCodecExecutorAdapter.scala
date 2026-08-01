package zarr4s

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/** Explicit bridge from a synchronous byte codec to the portable async runtime.
  *
  * The captured execution context must be dedicated to blocking codec work. The reader's callback
  * context is deliberately not used for the algorithm.
  */
object BlockingByteCodecExecutorAdapter:
  def apply(
      executor: SyncByteCodecExecutor,
      blockingExecutionContext: ExecutionContext
  ): AsyncByteCodecExecutor =
    new AsyncByteCodecExecutor:
      val name: String = executor.name

      def decode(
          codec: CompiledCodec,
          encoded: OwnedBytes,
          expectedDecoded: ByteCount,
          limits: DecodeLimits
      )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] =
        Future(executor.decode(codec, encoded, expectedDecoded, limits))(using
          blockingExecutionContext
        )

      def encode(
          codec: CompiledCodec,
          decoded: OwnedBytes
      )(using ExecutionContext): Future[Either[CodecError, OwnedBytes]] =
        Future(executor.encode(codec, decoded))(using blockingExecutionContext)

/** JVM compression runtime for [[AsyncZarr]] and other portable async interpreters. */
object JvmAsyncCodecRuntime:
  def portable(blockingExecutionContext: ExecutionContext): AsyncCodecRuntime =
    AsyncCodecRuntime.unsafe(
      "JVM asynchronous runtime",
      Vector(
        BlockingByteCodecExecutorAdapter(JvmGzip, blockingExecutionContext),
        BlockingByteCodecExecutorAdapter(JvmZlib, blockingExecutionContext)
      )
    )
