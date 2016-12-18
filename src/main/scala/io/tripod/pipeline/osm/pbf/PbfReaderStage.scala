package io.tripod.pipeline.osm.pbf

import java.beans.VetoableChangeListener
import java.nio.ByteOrder

import akka.stream.scaladsl.Flow
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage._
import akka.util.ByteString
import crosby.binary.fileformat.{Blob, BlobHeader}
import io.tripod.pipeline.osm.pbf.model.BlockType.{OSMData, OSMHeader}
import io.tripod.pipeline.osm.pbf.model.FileBlock

import scala.annotation.tailrec
import scala.concurrent.{Future, Promise}

class PbfReaderStage extends GraphStageWithMaterializedValue[FlowShape[ByteString, FileBlock], Future[Long]] {
  implicit val order = ByteOrder.BIG_ENDIAN
  val in             = Inlet[ByteString]("PbfReaderStage.in")
  val out            = Outlet[FileBlock]("PbfReaderStage.out")
  val shape          = FlowShape.of(in, out)

  override def createLogicAndMaterializedValue(attributes: Attributes): (GraphStageLogic, Future[Long]) = {
    val promise = Promise[Long]
    val logic = new GraphStageLogic(shape) {
      private[this] var buffer: ByteString                    = ByteString.empty
      private[this] var currentBlobHeader: Option[BlobHeader] = None
      private[this] var currentBlob: Option[Blob]             = None
      private[this] var sizeCounter                           = 0L
      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val bytes = grab(in)
          sizeCounter += bytes.length
          buffer = buffer ++ bytes
          run()
        }

        override def onUpstreamFinish(): Unit = {
          promise.success(sizeCounter)
          super.onUpstreamFinish()
        }
      })
      setHandler(out, new OutHandler {
        override def onPull(): Unit =
          if (isClosed(in)) run()
          else pull(in)
      })

      private def run() = {
        computeBloc(buffer, currentBlobHeader, currentBlob) match {
          case (b, None, None) ⇒
            buffer = b
            if (isClosed(in)) completeStage() else pull(in)
          case (b, Some(blobHeader), None) ⇒
            currentBlobHeader = Some(blobHeader)
            buffer = b
            if (isClosed(in)) completeStage() else pull(in)
          case (b, Some(blobHeader), Some(blob)) ⇒
            emit(out, blobHeader.`type` match {
              case "OSMHeader" => FileBlock(OSMHeader, blob)
              case "OSMData"   => FileBlock(OSMData, blob)
            })
            currentBlobHeader = None
            currentBlob = None
            buffer = b
          case _ => failStage(new UnsupportedOperationException())
        }
      }

      @tailrec
      private def computeBloc(buffer: ByteString,
                              currentBlobHeader: Option[BlobHeader],
                              currentBlob: Option[Blob]): (ByteString, Option[BlobHeader], Option[Blob]) = {
        (currentBlobHeader, currentBlob) match {
          case (None, None) ⇒
            if (buffer.length < 4)
              (buffer, None, None)
            else {
              val blocLength = buffer.iterator.getInt
              val blocBuffer = buffer.drop(4)
              if (blocBuffer.length < blocLength)
                (blocBuffer, None, None)
              else {
                val blobHeader =
                  Some(BlobHeader.parseFrom(blocBuffer.iterator.getBytes(blocLength)))
                computeBloc(blocBuffer.drop(blocLength), blobHeader, currentBlob)
              }
            }
          case (Some(blobHeader), None) ⇒
            if (buffer.length < blobHeader.datasize)
              (buffer, currentBlobHeader, None)
            else {
              val dataSize = blobHeader.datasize
              val blob =
                Some(Blob.parseFrom(buffer.iterator.getBytes(dataSize)))
              (buffer.drop(dataSize), currentBlobHeader, blob)
            }
          case _ ⇒ (buffer, currentBlobHeader, currentBlob)
        }
      }

    }
    (logic, promise.future)
  }
}

object PbfReaderStage {
  def apply() = Flow.fromGraph(new PbfReaderStage)
}
