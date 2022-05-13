package org.yupana.khipu

import scala.annotation.tailrec

class NodeBlock(override val id: Int, override val table: KTable) extends Block {

  lazy val children: List[NodeBlock.Child] = {
    (0 until numOfRecords).map { i =>
      val baseOffset = table.headerSize + i * Block.ID_BLOCK_SIZE * table.keySize * 2
      val id = StorageFormat.getInt(segment, baseOffset)
      val startKeyOffset = baseOffset + Block.ID_BLOCK_SIZE
      val startKey = StorageFormat.getBytes(segment, startKeyOffset, table.keySize)
      val endKeyOffset = startKeyOffset + table.keySize
      val endKey = StorageFormat.getBytes(segment, endKeyOffset, table.keySize)
      NodeBlock.Child(id, startKey, endKey)
    }.toList
  }

  val maxNumOfChildren: Int = (Block.BLOCK_SIZE - keySize) / Block.ID_BLOCK_SIZE

  override def put(rows: Seq[Row]): List[NodeBlock] = {
    val blocks = doPut(rows, children, Nil)
      .grouped(maxNumOfChildren)
      .map { gr =>
        val s = gr.head.startKey
        val e = gr.last.endKey
        val id = NodeBlock.write(table, s, e, gr)
        new NodeBlock(id, table)
      }
      .toList

    blocks
  }

  @tailrec
  private def doPut(rows: Seq[Row], nodes: Seq[NodeBlock.Child], result: Seq[NodeBlock.Child]): Seq[NodeBlock.Child] = {
    nodes match {
      case child :: tail =>
        val (childRows, tailRows) = rows.span(r => StorageFormat.BYTES_COMPARATOR.compare(r.key, child.endKey) <= 0)
        val blocks = if (childRows.nonEmpty) {
          val childBlock = table.block(child.id)
          val bs = childBlock.put(childRows)
          bs.map { b =>
            NodeBlock.Child(b.id, b.startKey, b.endKey)
          }
        } else {
          Seq(child)
        }

        doPut(tailRows, tail, blocks ++ result)

      case Nil => result
    }
  }
}

object NodeBlock {

  case class Child(id: Int, startKey: Array[Byte], endKey: Array[Byte])

  def write(table: KTable, startKey: Array[Byte], endKey: Array[Byte], children: Seq[Child]): Int = {

    val id = table.allocateBlock
    val segment = table.blockSegment(id)

    Block.writeHeader(
      segment,
      id,
      Block.NODE_KIND,
      table.keySize,
      startKey,
      endKey,
      children.size,
      children.size * (Block.ID_BLOCK_SIZE + table.keySize)
    )

    children.zipWithIndex.foreach {
      case (b, i) =>
        val baseOffset = table.headerSize + i * Block.ID_BLOCK_SIZE * table.keySize * 2
        StorageFormat.setInt(b.id, segment, baseOffset)
        val startKeyOffset = baseOffset + Block.ID_BLOCK_SIZE
        StorageFormat.setBytes(b.startKey, 0, segment, startKeyOffset, table.keySize)
        val endKeyOffset = startKeyOffset + table.keySize
        StorageFormat.setBytes(b.endKey, 0, segment, endKeyOffset, table.keySize)
    }
    id
  }

}
