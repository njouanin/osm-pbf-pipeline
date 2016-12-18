package io.tripod.pipeline.osm.pbf.model

import crosby.binary.fileformat.Blob

sealed trait BlockType
object BlockType {
  case object OSMHeader extends BlockType
  case object OSMData   extends BlockType
}

case class FileBlock(blockType: BlockType, blob: Blob)
