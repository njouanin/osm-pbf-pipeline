/*
 *********************************************************************************
 * "THE BEER-WARE LICENSE" (Revision 42):
 * <nico@beerfactory.org> wrote this file.  As long as you retain this notice you
 * can do whatever you want with this stuff. If we meet some day, and you think
 * this stuff is worth it, you can buy me a beer in return.   Nicolas JOUANIN
 *********************************************************************************
 */
package io.tripod.pipeline.osm.pbf

import java.util.zip.Inflater

import akka.stream.FlowShape
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge}
import com.google.protobuf.ByteString
import io.tripod.pipeline.osm.pbf.model.FileBlock

import scala.concurrent.{ExecutionContext, Future}

object UncompressStage {

  /**
    * Create a UncompressStage
    * @param parallelism degree of blob uncompression parallelism
    * @param ec
    * @return
    */
  def apply(parallelism: Int)(implicit ec: ExecutionContext) =
    Flow.fromGraph(GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._

      val broadcast = b.add(Broadcast[FileBlock](2))
      val merge = b.add(Merge[FileBlock](2))

      broadcast.out(0).filter(_.blob.raw.isDefined) ~> merge.in(0)
      broadcast
        .out(1)
        .filter(_.blob.zlibData.isDefined)
        .mapAsync(parallelism)(block =>
          Future {
            val inflater = new Inflater()
            val blob = block.blob
            val decompressedData = new Array[Byte](blob.rawSize.get)
            inflater.setInput(blob.zlibData.get.toByteArray)
            inflater.inflate(decompressedData)
            inflater.end()
            block.copy(blob = blob.clearZlibData.withRaw(
              ByteString.copyFrom(decompressedData)))
        }) ~> merge.in(1)

      FlowShape(broadcast.in, merge.out)
    })
}
