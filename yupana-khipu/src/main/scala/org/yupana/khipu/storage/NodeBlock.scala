package org.yupana.khipu.storage

import org.yupana.khipu.KhipuMetricCollector.Metrics
import org.yupana.khipu.storage.NodeBlock.childSize

import java.lang.foreign.MemorySegment
import scala.annotation.tailrec

class NodeBlock(override val id: Int, override val table: KTable) extends Block {

  def children: Iterator[NodeBlock.Child] = {
    (0 until numOfRecords).iterator.map { i =>
      val baseOffset = Block.headerSize(keySize) + i * childSize(keySize)
      val id = StorageFormat.getInt(segment, baseOffset)
      val startKeyOffset = baseOffset + StorageFormat.alignLong(Block.BLOCK_ID_SIZE)
      val startKey = StorageFormat.getSegment(segment, startKeyOffset, table.keySize)
      val endKeyOffset = StorageFormat.alignLong(startKeyOffset + table.keySize)
      val endKey = StorageFormat.getSegment(segment, endKeyOffset, table.keySize)
      NodeBlock.Child(id, startKey, endKey)
    }
  }

  override def put(rows: Seq[Row]): List[NodeBlock] = {
    val newChildren = doPut(rows, children.toSeq, Nil)
    val blocks = NodeBlock.splitAndWrite(table, newChildren, startKey, endKey).toList
    table.freeBlock(id)
    blocks
  }

  @tailrec
  private def doPut(rows: Seq[Row], nodes: Seq[NodeBlock.Child], result: Seq[NodeBlock.Child]): Seq[NodeBlock.Child] = {
    nodes.toList match {
      case child :: tail =>
        val (childRows, tailRows) = rows.span(r => StorageFormat.SEGMENT_COMPARATOR.compare(r.key, child.endKey) <= 0)
        val blocks = if (childRows.nonEmpty) {
          val childBlock = table.block(child.id)
          val bs = childBlock.put(childRows)
          bs.map { b =>
            NodeBlock.Child(b.id, b.startKey, b.endKey)
          }
        } else {
          Seq(child)
        }

        doPut(tailRows, tail, result ++ blocks)

      case Nil => result
    }
  }
}

object NodeBlock {

  case class Child(id: Int, startKey: MemorySegment, endKey: MemorySegment)

  def write(table: KTable, children: Seq[Child], startKey: MemorySegment, endKey: MemorySegment): Int = {
    val id = table.allocateBlock
    val segment = table.blockSegment(id)
    write(id, segment, children, table.keySize, startKey, endKey)
  }

  def write(
      id: Int,
      blockSegment: MemorySegment,
      children: Seq[Child],
      keySize: Int,
      startKey: MemorySegment,
      endKey: MemorySegment
  ): Int = {
    Metrics.writeNodeBlock.measure(1) {

      require(
        startKey.byteSize() == endKey.byteSize() && startKey.byteSize() == keySize,
        "Wrong block format, size of keys is not equals"
      )

      Block.writeHeader(
        blockSegment,
        id,
        Block.NODE_KIND,
        keySize,
        startKey,
        endKey,
        children.size,
        children.size * (Block.BLOCK_ID_SIZE + keySize)
      )

      children.zipWithIndex.foreach {
        case (b, i) =>
          val baseOffset = Block.headerSize(keySize) + i * childSize(keySize)
          StorageFormat.setInt(b.id, blockSegment, baseOffset)
          val startKeyOffset = baseOffset + StorageFormat.alignLong(Block.BLOCK_ID_SIZE)
          StorageFormat.copy(b.startKey, blockSegment, startKeyOffset)
          val endKeyOffset = StorageFormat.alignLong(startKeyOffset + keySize)
          StorageFormat.copy(b.endKey, blockSegment, endKeyOffset)
      }
      id
    }
  }

  def childSize(keySize: Int): Int = {
    StorageFormat.alignLong(Block.BLOCK_ID_SIZE) + StorageFormat.alignLong(keySize) * 2
  }

  private def maxNumOfChildren(keySize: Int): Int = {
    (Block.BLOCK_SIZE - Block.headerSize(keySize)) / NodeBlock.childSize(keySize)
  }

  private def block2children(blocks: Seq[Block]) = {
    blocks.map { b =>
      NodeBlock.Child(b.id, b.startKey, b.endKey)
    }
  }

  def apply(
      table: KTable,
      children: Seq[NodeBlock.Child],
      startKey: MemorySegment,
      endKey: MemorySegment
  ): NodeBlock = {
    val id = NodeBlock.write(table, children, startKey, endKey)
    new NodeBlock(id, table)
  }

  def splitAndWriteBlocks(
      table: KTable,
      blocks: Seq[Block],
      start: MemorySegment,
      end: MemorySegment
  ): Seq[NodeBlock] = {
    val children = block2children(blocks)
    splitAndWrite(table, children, start, end)
  }

  def splitAndWrite(
      table: KTable,
      newChildren: Seq[NodeBlock.Child],
      start: MemorySegment,
      end: MemorySegment
  ): Seq[NodeBlock] = {
    val s = newChildren.size
    if (s <= maxNumOfChildren(start.byteSize().toInt)) {
      Seq(NodeBlock(table, newChildren, start, end))
    } else {
      val (left, right) = newChildren.splitAt(s / 2)
      val middle = left.last.endKey
      val incMiddle = StorageFormat.incMemorySegment(middle, start.byteSize().toInt)
      splitAndWrite(table, left, start, middle) ++ splitAndWrite(table, right, incMiddle, end)
    }
  }

}
