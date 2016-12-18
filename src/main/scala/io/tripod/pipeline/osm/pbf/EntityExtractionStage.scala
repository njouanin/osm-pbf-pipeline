/*
 *********************************************************************************
 * "THE BEER-WARE LICENSE" (Revision 42):
 * <nico@beerfactory.org> wrote this file.  As long as you retain this notice you
 * can do whatever you want with this stuff. If we meet some day, and you think
 * this stuff is worth it, you can buy me a beer in return.   Nicolas JOUANIN
 *********************************************************************************
 */
package io.tripod.pipeline.osm.pbf

import java.time.Instant

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.scaladsl.{Balance, Flow, GraphDSL, Merge}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import crosby.binary.osmformat.{Info, PrimitiveBlock, PrimitiveGroup}
import io.tripod.pipeline.osm.pbf.model._

class EntityExtractionStage extends GraphStage[FlowShape[PrimitiveBlock, OSMEntity]] {

  val in  = Inlet[PrimitiveBlock]("EntityExtractionStage.in")
  val out = Outlet[OSMEntity]("EntityExtractionStage.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with Utilities {
      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val block = grab(in)
          emitMultiple(out, extractEntities(block))
        }
      })
      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })

      private def extractEntities(block: PrimitiveBlock): Iterator[OSMEntity] = {
        import io.tripod.pipeline.osm.pbf.PrimitiveGroupType._
        block.primitivegroup.flatMap { group ⇒
          detectType(group) match {
            case Nodes      ⇒ extractNodes(block, group)
            case DenseNodes ⇒ extractDenseNodes(block, group)
            case Ways       => extractWays(block, group)
            case Relations  => extractRelations(block, group)
            case _          ⇒ Seq.empty
          }
        }.toIterator
      }

      private def extractInfo(info: Option[Info],
                              timestamp: Long ⇒ Long,
                              stringTable: Int ⇒ String): Option[EntityInfo] =
        info.map { info ⇒
          EntityInfo(
            version = info.version,
            timestamp = info.timestamp.map(ts ⇒ Instant.ofEpochMilli(timestamp(ts))),
            changeset = info.changeset,
            uid = info.uid,
            userSid = info.userSid,
            user = info.userSid.map(sid => stringTable(sid)),
            visibility = info.visible
          )
        }

      private def extractNodes(block: PrimitiveBlock, group: PrimitiveGroup): Seq[NodeEntity] = {
        def latitude: Long ⇒ Double  = extractCoordinate(block.getLatOffset, block.getGranularity)
        def longitude: Long ⇒ Double = extractCoordinate(block.getLonOffset, block.getGranularity)
        def timestamp: Long ⇒ Long   = extractTimestamp(block.getDateGranularity)
        def getString: Int ⇒ String  = stringTableAccessor(block.stringtable)

        group.nodes.map { node ⇒
          NodeEntity(
            id = node.id,
            lat = latitude(node.lat),
            long = longitude(node.lon),
            tags = node.keys.zip(node.vals).map(tag => (getString(tag._1), getString(tag._2))).toMap,
            info = extractInfo(node.info, timestamp, getString)
          )

        }
      }

      private def extractRelations(block: PrimitiveBlock, group: PrimitiveGroup): Seq[RelationEntity] = {
        def getString: Int ⇒ String = stringTableAccessor(block.stringtable)
        def timestamp: Long ⇒ Long  = extractTimestamp(block.getDateGranularity)
        group.relations.map { relation ⇒
          RelationEntity(
            id = relation.id,
            tags = relation.keys.zip(relation.vals).map(tag => (getString(tag._1), getString(tag._2))).toMap,
            info = extractInfo(relation.info, timestamp, getString),
            members = relation.memids
              .zip(relation.rolesSid)
              .zip(relation.types)
              .scanLeft(MemberEntity(0L, MemberType.Node, "")) {
                case (acc, ((memId, roleId), memType)) ⇒
                  MemberEntity(
                    ref = acc.ref + memId,
                    memberType = memType match {
                      case m if m.isNode     ⇒ MemberType.Node
                      case m if m.isWay      ⇒ MemberType.Way
                      case m if m.isRelation ⇒ MemberType.Relation
                    },
                    role = getString(roleId)
                  )
              }
              .drop(1)
          )
        }
      }

      private def extractWays(block: PrimitiveBlock, group: PrimitiveGroup): Seq[WayEntity] = {
        def timestamp: Long ⇒ Long  = extractTimestamp(block.getDateGranularity)
        def getString: Int ⇒ String = stringTableAccessor(block.stringtable)

        group.ways.map { way ⇒
          WayEntity(
            id = way.id,
            tags = way.keys.zip(way.vals).map(tag => (getString(tag._1), getString(tag._2))).toMap,
            refs = way.refs.scanLeft(0l) { _ + _ }.drop(1),
            info = extractInfo(way.info, timestamp, getString)
          )
        }
      }

      private def extractDenseNodes(block: PrimitiveBlock, group: PrimitiveGroup): Seq[NodeEntity] = {
        require(group.dense.isDefined)
        case class ZippedNode(id: Long, lat: Long, long: Long)
        case class ZippedInfo(version: Int, timestamp: Long, changeset: Long, uid: Int, userSid: Int, visible: Boolean)
        def timestamp: Long ⇒ Long  = extractTimestamp(block.getDateGranularity)
        def getString: Int ⇒ String = stringTableAccessor(block.stringtable)
        def extractInfo(zInfo: Option[ZippedInfo], accumulator: Option[EntityInfo]) = {
          zInfo.map { z =>
            EntityInfo(
              version = Some(z.version),
              timestamp = accumulator.map(_.timestamp.getOrElse(Instant.EPOCH).plusMillis(timestamp(z.timestamp))),
              changeset = accumulator.map(_.changeset.getOrElse(0L) + z.changeset),
              uid = accumulator.map(_.uid.getOrElse(0) + z.uid),
              userSid = accumulator.map(_.userSid.getOrElse(0) + z.userSid),
              user = accumulator.map(acc ⇒ getString(acc.userSid.getOrElse(0) + z.userSid)),
              visibility = Some(z.visible)
            )
          }
        }

        val denseNodes   = group.dense.get
        val tagsIterator = denseNodes.keysVals.toIterator

        val zippedDenseInfo = denseNodes.denseinfo.map { denseInfo =>
          denseInfo.version
            .zip(denseInfo.timestamp)
            .zip(denseInfo.changeset)
            .zip(denseInfo.uid)
            .zip(denseInfo.userSid)
            .zipAll(denseInfo.visible, ((((0, 0L), 0L), 0), 0), true)
            .map {
              case (((((version, timestamp), changeset), uid), userSid), visible) =>
                Some(ZippedInfo(version, timestamp, changeset, uid, userSid, visible))
            }
        }.getOrElse(Seq.empty)

        val zippedNodes = denseNodes.id
          .zip(denseNodes.lat)
          .zip(denseNodes.lon)
          .map {
            case ((id, lat), lon) ⇒ ZippedNode(id, lat, lon)
          }
          .zipAll(zippedDenseInfo, ZippedNode(0, 0, 0), None)

        zippedNodes
          .scanLeft(NodeEntity(0L, 0.0, 0.0, Map.empty, Some(EntityInfo(None, None, None, None, None, None, None)))) {
            case (acc, (zNode, zInfo)) ⇒
              val tags = tagsIterator
                .takeWhile(_ != 0L)
                .grouped(2)
                .map { (tag) =>
                  getString(tag.head) -> getString(tag.last)
                }
                .toMap
              acc.copy(id = acc.id + zNode.id,
                       lat = extractCoordinate(block.getLatOffset, block.getGranularity, acc.lat)(zNode.lat),
                       long = extractCoordinate(block.getLatOffset, block.getGranularity, acc.long)(zNode.long),
                       info = extractInfo(zInfo, acc.info),
                       tags = tags)
          }
          .drop(1)
      }
    }
}

object EntityExtractionStage {
  def apply() = new EntityExtractionStage()

  def apply(nbWorkers: Int) =
    Flow.fromGraph(GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._

      val balance = b.add(Balance[PrimitiveBlock](nbWorkers))
      val merge   = b.add(Merge[OSMEntity](nbWorkers))
      for (i <- 0 to nbWorkers - 1) {
        val stage = b.add(new EntityExtractionStage())
        balance.out(i) ~> stage ~> merge.in(i)
      }
      FlowShape(balance.in, merge.out)
    })
}
