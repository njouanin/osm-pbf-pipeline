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
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.io.Tcp
import akka.stream.{ActorMaterializer, FlowShape}
import akka.stream.scaladsl.{Broadcast, FileIO, Flow, GraphDSL, Keep, Merge, Sink, Source}
import io.tripod.pipeline.osm.pbf.PbfStreamFlow
import io.tripod.pipeline.osm.pbf.model.{NodeEntity, OSMEntity, RelationEntity, WayEntity}

import scala.concurrent.Future

object EntityCounter extends App {
  val PARALLELISM    = 2
  val PBF_SOURCE_URL = "http://download.geofabrik.de/europe/monaco-latest.osm.pbf"

  implicit val system       = ActorSystem("EntityCounter")
  implicit val materializer = ActorMaterializer()
  implicit val ec           = system.dispatcher

  val partialFlow = Flow.fromGraph(GraphDSL.create() { implicit builder ⇒
    import GraphDSL.Implicits._

    val broadcast = builder.add(Broadcast[OSMEntity](3))
    val osmFlow   = builder.add(PbfStreamFlow(PARALLELISM))
    val merge     = builder.add(Merge[(String, Int)](3))

    osmFlow.out ~> broadcast.in
    broadcast
      .out(0)
      .filter(_.isInstanceOf[NodeEntity])
      .fold(0)((acc, _) => acc + 1)
      .map(count => ("NodeEntity", count)) ~> merge.in(0)
    broadcast
      .out(1)
      .filter(_.isInstanceOf[WayEntity])
      .fold(0)((acc, _) => acc + 1)
      .map(count => ("WayEntity", count)) ~> merge.in(1)
    broadcast
      .out(2)
      .filter(_.isInstanceOf[RelationEntity])
      .fold(0)((acc, _) => acc + 1)
      .map(count => ("RelationEntity", count)) ~> merge.in(2)

    FlowShape(osmFlow.in, merge.out)
  })

  val start = System.currentTimeMillis()
  val responseFuture: Future[HttpResponse] =
    Http().singleRequest(HttpRequest(uri = PBF_SOURCE_URL))

  responseFuture.flatMap { response =>
    response.entity
      .withSizeLimit(Long.MaxValue)
      .dataBytes
      .via(partialFlow)
      .toMat(Sink.foreach(t ⇒ println(s"${t._1} -> ${t._2}")))(Keep.right)
      .run()
      .flatMap {
        case _ ⇒
          val t = System.currentTimeMillis() - start
          println(s"Stream process time: $t ms")
          system.terminate()
      }
  }
}
