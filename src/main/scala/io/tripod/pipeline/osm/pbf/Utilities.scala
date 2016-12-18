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

import crosby.binary.osmformat.{PrimitiveGroup, StringTable}

object PrimitiveGroupType extends Enumeration {
  type PrimitiveGroupType = Value
  val Relations, Nodes, Ways, ChangeSets, DenseNodes, Unknown = Value
}

trait Utilities {
  import PrimitiveGroupType._

  def detectType(group: PrimitiveGroup): PrimitiveGroupType = group match {
    case _ if group.relations.nonEmpty => Relations
    case _ if group.nodes.nonEmpty     => Nodes
    case _ if group.ways.nonEmpty      => Ways
    case _ if group.dense.isDefined    => DenseNodes
    case _                             => Unknown
  }

  /**
    * Extract a coordinate (lat/long) using the PBF file format formula
    * @param offset
    * @param granularity
    * @param coord
    * @return
    */
  def extractCoordinate(offset: Long, granularity: Int, delta: Double = 0.0)(coord: Long): Double = {
    ((BigDecimal(.000000001) * (offset + (granularity * coord))) + delta).toDouble
  }
  def extractTimestamp(granularity: Int)(timeStamp: Long): Long = timeStamp * granularity
  def stringTableAccessor(table: StringTable)(index: Int)       = table.s(index).toStringUtf8

}
