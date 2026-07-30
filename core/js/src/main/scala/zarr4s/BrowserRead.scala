package zarr4s

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/** Scala.js compatibility names for the portable asynchronous reader. */
type BrowserOpenedArray = AsyncOpenedArray
type BrowserOpenedGroup = AsyncOpenedGroup
type BrowserOpenedNode = AsyncOpenedNode

object BrowserOpenedNode:
  export AsyncOpenedNode.*

/** Browser-oriented facade that adds the browser gzip executor by default.
  *
  * The reader implementation itself is portable and lives in [[AsyncZarr]].
  */
object BrowserZarr:
  def openArray(
      store: AsyncObjectReader,
      path: ZarrPath = ZarrPath.root,
      capabilities: ZarrCapabilities = ZarrCapabilities(),
      limits: OpenLimits = OpenLimits(),
      runtime: AsyncCodecRuntime = BrowserCodecRuntime.portable,
      consolidation: ConsolidationMode = ConsolidationMode.Prefer
  )(using ExecutionContext): Future[Either[ZarrError, BrowserOpenedArray]] =
    AsyncZarr.openArray(store, path, capabilities, limits, runtime, consolidation)

  def openGroup(
      store: AsyncObjectReader,
      path: ZarrPath = ZarrPath.root,
      capabilities: ZarrCapabilities = ZarrCapabilities(),
      limits: OpenLimits = OpenLimits(),
      runtime: AsyncCodecRuntime = BrowserCodecRuntime.portable,
      consolidation: ConsolidationMode = ConsolidationMode.Prefer
  )(using ExecutionContext): Future[Either[ZarrError, BrowserOpenedGroup]] =
    AsyncZarr.openGroup(store, path, capabilities, limits, runtime, consolidation)

  def openNode(
      store: AsyncObjectReader,
      path: ZarrPath = ZarrPath.root,
      capabilities: ZarrCapabilities = ZarrCapabilities(),
      limits: OpenLimits = OpenLimits(),
      runtime: AsyncCodecRuntime = BrowserCodecRuntime.portable,
      consolidation: ConsolidationMode = ConsolidationMode.Prefer
  )(using ExecutionContext): Future[Either[ZarrError, BrowserOpenedNode]] =
    AsyncZarr.openNode(store, path, capabilities, limits, runtime, consolidation)
