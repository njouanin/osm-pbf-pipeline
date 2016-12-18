package io.tripod.pipeline.osm.pbf.model

import crosby.binary.fileformat.Blob

/**
  * Created by nicolas.jouanin on 13/12/16.
  */
sealed trait BlockType
object BlockType {
  case object OSMHeader extends BlockType
  case object OSMData extends BlockType
}

case class FileBlock(blockType: BlockType, blob: Blob)
