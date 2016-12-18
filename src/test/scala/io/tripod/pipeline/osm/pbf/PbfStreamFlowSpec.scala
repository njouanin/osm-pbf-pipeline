package io.tripod.pipeline.osm.pbf

import java.nio.file.Paths
import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Sink}
import io.tripod.pipeline.osm.pbf.model._
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}

class PbfStreamFlowSpec extends WordSpec with Matchers with ScalaFutures with IntegrationPatience {
  implicit val system       = ActorSystem("PbfStreamFlowSpec")
  implicit val materializer = ActorMaterializer()
  implicit val ec           = system.dispatcher

  "Monaco Stream source" should {
    val source       = FileIO.fromPath(Paths.get(this.getClass.getResource("/monaco-latest.osm.pbf").toURI))
    val monacoSource = PbfStreamFlow.from(source, 4)
    "Read an expected dense node" in {
      val result =
        monacoSource
          .filter(_.isInstanceOf[NodeEntity])
          .filter(_.asInstanceOf[NodeEntity].id == 21911863L)
          .runWith(Sink.seq)
          .futureValue
      result should have length 1
      val node = result.head.asInstanceOf[NodeEntity]
      node.lat shouldEqual 43.7370125
      node.long shouldEqual 7.422028
      node.info.map { info ⇒
        info.version shouldEqual Some(5)
        info.timestamp shouldEqual Some(Instant.parse("2012-05-02T14:50:31Z"))
        info.changeset shouldEqual Some(11480240)
        info.uid shouldEqual Some(378737)
        info.user shouldEqual Some("Scrup")
      }
    }
    "Read an expected delta-mode dense node" in {
      val result =
        monacoSource
          .filter(_.isInstanceOf[NodeEntity])
          .filter(_.asInstanceOf[NodeEntity].id == 21911883L)
          .runWith(Sink.seq)
          .futureValue
      result should have length 1
      val node = result.head.asInstanceOf[NodeEntity]
      node.lat shouldEqual 43.7371175
      node.long shouldEqual 7.4229093
      node.info.map { info ⇒
        info.version shouldEqual Some(6)
        info.timestamp shouldEqual Some(Instant.parse("2012-05-01T15:06:19Z"))
        info.changeset shouldEqual Some(11470653)
        info.uid shouldEqual Some(378737)
        info.user shouldEqual Some("Scrup")
      }
    }
    "Read an expected node with tags" in {
      val result =
        monacoSource
          .filter(_.isInstanceOf[NodeEntity])
          .filter(_.asInstanceOf[NodeEntity].id == 21911888L)
          .runWith(Sink.seq)
          .futureValue
      result should have length 1
      val node = result.head.asInstanceOf[NodeEntity]
      node.tags should contain("highway"      → "crossing")
      node.tags should contain("crossing_ref" → "zebra")
    }
    "Read an expected way" in {
      val result =
        monacoSource
          .filter(_.isInstanceOf[WayEntity])
          .filter(_.asInstanceOf[WayEntity].id == 4097656L)
          .runWith(Sink.seq)
          .futureValue
      result should have length 1
      val way = result.head.asInstanceOf[WayEntity]
      way.refs should contain(21912089)
      way.refs should contain(1079750744)
      way.refs should contain(2104793864)
      way.refs should contain(1110560507)
      way.refs should contain(21912093)
      way.refs should contain(21912095)
      way.refs should contain(1079751630)
      way.refs should contain(21912097)
      way.refs should contain(21912099)
      way.tags should contain("name"    → "Avenue Princesse Alice")
      way.tags should contain("lanes"   → "2")
      way.tags should contain("oneway"  → "yes")
      way.tags should contain("highway" → "primary")
      way.info.map { info ⇒
        info.version shouldEqual Some(8)
        info.timestamp shouldEqual Some(Instant.parse("2015-07-25T08:49:15Z"))
        info.changeset shouldEqual Some(32866762)
        info.uid shouldEqual Some(217070)
        info.user shouldEqual Some("Davio")
      }
    }
    "Read an expected relation" in {
      val result =
        monacoSource
          .filter(_.isInstanceOf[RelationEntity])
          .filter(_.asInstanceOf[RelationEntity].id == 174958L)
          .runWith(Sink.seq)
          .futureValue
      result should have length 1
      val relation = result.head.asInstanceOf[RelationEntity]
      relation.members should contain(MemberEntity(37811840, MemberType.Way, "outer"))
      relation.members should contain(MemberEntity(37811852, MemberType.Way, "outer"))
      relation.members should contain(MemberEntity(37811853, MemberType.Way, "outer"))
      relation.members should contain(MemberEntity(37811849, MemberType.Way, "outer"))
      relation.members should contain(MemberEntity(37811847, MemberType.Way, "outer"))
      relation.members should contain(MemberEntity(37811850, MemberType.Way, "outer"))
      relation.members should contain(MemberEntity(26694373, MemberType.Node, "admin_centre"))
      relation.tags should contain("addr:postcode"        → "06320")
      relation.tags should contain("admin_level"          → "8")
      relation.tags should contain("boundary"             → "administrative")
      relation.tags should contain("name"                 → "La Turbie")
      relation.tags should contain("name:it"              → "Turbia")
      relation.tags should contain("ref:INSEE"            → "06150")
      relation.tags should contain("source:addr:postcode" → "source of postcode is from osm nodes")
      relation.tags should contain("type"                 → "boundary")
      relation.tags should contain("wikidata"             → "Q244709")
      relation.tags should contain("wikipedia"            → "fr:La Turbie")
      relation.info.map { info ⇒
        info.version shouldEqual Some(13)
        info.timestamp shouldEqual Some(Instant.parse("2016-12-03T06:44:50Z"))
        info.changeset shouldEqual Some(44130518)
        info.uid shouldEqual Some(339581)
        info.user shouldEqual Some("nyuriks")
      }
    }
  }
}
