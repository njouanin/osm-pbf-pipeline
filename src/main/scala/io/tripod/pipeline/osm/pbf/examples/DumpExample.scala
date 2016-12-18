/*
 *********************************************************************************
 * "THE BEER-WARE LICENSE" (Revision 42):
 * <nico@beerfactory.org> wrote this file.  As long as you retain this notice you
 * can do whatever you want with this stuff. If we meet some day, and you think
 * this stuff is worth it, you can buy me a beer in return.   Nicolas JOUANIN
 *********************************************************************************
 */
package io.tripod.pipeline.osm.pbf.examples

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink}
import io.tripod.pipeline.osm.pbf.PbfStreamFlow

import scala.concurrent.Future

object DumpExample extends App {
  val PARALLELISM    = 2
  val PBF_SOURCE_URL = "http://download.geofabrik.de/europe/france-latest.osm.pbf"

  implicit val system       = ActorSystem("EntityCounter")
  implicit val materializer = ActorMaterializer()
  implicit val ec           = system.dispatcher

  val start = System.currentTimeMillis()
  val responseFuture: Future[HttpResponse] =
    Http().singleRequest(HttpRequest(uri = PBF_SOURCE_URL))

  responseFuture.flatMap { response =>
    response.entity
      .withSizeLimit(Long.MaxValue)
      .dataBytes
      .via(PbfStreamFlow(PARALLELISM))
      .toMat(Sink.foreach(println))(Keep.right)
      .run()
      .flatMap {
        case _ ⇒
          val t = System.currentTimeMillis() - start
          println(s"Stream process time: $t ms")
          system.terminate()
      }
  }

}
