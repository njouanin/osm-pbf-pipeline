package io.tripod.pipeline.osm.pbf

import java.time.Instant

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.scaladsl.Flow
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler}
import crosby.binary.osmformat.{HeaderBlock, PrimitiveBlock}
import io.tripod.pipeline.osm.pbf.model.{BlockType, FileBlock, HeaderEntity}

import scala.concurrent.{ExecutionContext, Future, Promise}

class HeaderDecoderStage(implicit ec: ExecutionContext)
    extends GraphStageWithMaterializedValue[FlowShape[FileBlock, FileBlock], Future[HeaderEntity]] {

  val in    = Inlet[FileBlock]("EntityDecoderStage.in")
  val out   = Outlet[FileBlock]("EntityDecoderStage.out")
  val shape = FlowShape.of(in, out)

  override def createLogicAndMaterializedValue(attributes: Attributes): (GraphStageLogic, Future[HeaderEntity]) = {
    val promise = Promise[HeaderEntity]
    val logic = new GraphStageLogic(shape) {
      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val elem = grab(in)
          elem match {
            case FileBlock(BlockType.OSMHeader, blob) =>
              promise.completeWith(Future {
                val block = HeaderBlock.parseFrom(blob.raw.get.toByteArray)
                HeaderEntity(requiredFeatures = block.requiredFeatures,
                             optionalFeatures = block.optionalFeatures,
                             writingProgram = block.writingprogram,
                             source = block.source,
                             osmosisReplicationTimestamp =
                               block.osmosisReplicationTimestamp.map(ts => Instant.ofEpochSecond(ts)),
                             osmosisReplicationBaseUrl = block.osmosisReplicationBaseUrl,
                             osmosisReplicationSequenceNumber = block.osmosisReplicationSequenceNumber)
              })
              pull(in)
            case block @ FileBlock(BlockType.OSMData, _) => emit(out, block)
          }
        }
      })
      setHandler(out, new OutHandler {
        override def onPull(): Unit = pull(in)
      })
    }
    (logic, promise.future)
  }
}

object FileBlockDecodeStage {
  def apply(parallelism: Int)(implicit ec: ExecutionContext): Flow[FileBlock, PrimitiveBlock, Future[HeaderEntity]] = {
    Flow
      .fromGraph(new HeaderDecoderStage)
      .mapAsync(parallelism)(block =>
        Future {
          PrimitiveBlock.parseFrom(block.blob.raw.get.toByteArray)
      })
  }
}
