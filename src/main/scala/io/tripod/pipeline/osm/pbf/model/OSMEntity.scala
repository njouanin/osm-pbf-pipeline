package io.tripod.pipeline.osm.pbf.model

import java.time.Instant

import io.tripod.pipeline.osm.pbf.model.MemberType.MemberType

case class EntityInfo(version: Option[Int],
                      timestamp: Option[Instant],
                      changeset: Option[Long],
                      uid: Option[Int],
                      userSid: Option[Int],
                      user: Option[String],
                      visibility: Option[Boolean])

trait OSMEntity {
  def tags: Map[String, String]
  def info: Option[EntityInfo]
}

case class HeaderEntity(requiredFeatures: Seq[String],
                        optionalFeatures: Seq[String],
                        writingProgram: Option[String],
                        source: Option[String],
                        osmosisReplicationTimestamp: Option[Instant],
                        osmosisReplicationSequenceNumber: Option[Long],
                        osmosisReplicationBaseUrl: Option[String])
case class NodeEntity(id: Long, lat: Double, long: Double, tags: Map[String, String], info: Option[EntityInfo])
    extends OSMEntity

case class WayEntity(id: Long, tags: Map[String, String], info: Option[EntityInfo], refs: Seq[Long]) extends OSMEntity

object MemberType extends Enumeration {
  type MemberType = Value
  val Node, Way, Relation = Value
}

case class MemberEntity(ref: Long, memberType: MemberType, role: String)

case class RelationEntity(id: Long, tags: Map[String, String], info: Option[EntityInfo], members: Seq[MemberEntity])
    extends OSMEntity
