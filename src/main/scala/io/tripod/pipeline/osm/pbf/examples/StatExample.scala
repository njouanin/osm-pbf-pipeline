/*
 *********************************************************************************
 * "THE BEER-WARE LICENSE" (Revision 42):
 * <nico@beerfactory.org> wrote this file.  As long as you retain this notice you
 * can do whatever you want with this stuff. If we meet some day, and you think
 * this stuff is worth it, you can buy me a beer in return.   Nicolas JOUANIN
 *********************************************************************************
 */
package io.tripod.pipeline.osm.pbf.examples

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import io.tripod.pipeline.osm.pbf._

import scala.concurrent.Future

object StatExample extends App {
  implicit val system       = ActorSystem("Example1")
  implicit val materializer = ActorMaterializer()
  implicit val ec           = system.dispatcher

  val source =
    FileIO.fromPath(Paths.get(this.getClass.getResource("/monaco-latest.osm.pbf").toURI))

  val ((sizeCount, headerFuture), doneFuture) = source
    .viaMat(PbfReaderStage())(Keep.right)
    .viaMat(UncompressStage(2))(Keep.left)
    .viaMat(FileBlockDecodeStage(2))(Keep.both)
    .viaMat(EntityExtractionStage(2))(Keep.left)
    .toMat(Sink.ignore)(Keep.both)
    .run()

  val ret = for {
    size   <- sizeCount
    header <- headerFuture
  } yield (size, header)

  ret.flatMap { r =>
    println(s"Total read: ${r._1} bytes")
    println(s"Header: ${r._2}")
    Future.successful()
  }

  doneFuture.flatMap(_ ⇒ system.terminate)
}
