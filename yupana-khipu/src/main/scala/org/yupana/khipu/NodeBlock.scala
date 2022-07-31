package org.yupana.khipu

import jdk.incubator.foreign.MemorySegment
import org.yupana.khipu.KhipuMetricCollector.Metrics

import scala.annotation.tailrec

class NodeBlock(override val id: Int, override val table: KTable) extends Block {

  lazy val children: List[NodeBlock.Child] = {
    (0 until numOfRecords).map { i =>
      val baseOffset = table.headerSize + i * Block.BLOCK_ID_SIZE * table.keySize * 2
      val id = StorageFormat.getInt(segment, baseOffset)
      val startKeyOffset = baseOffset + Block.BLOCK_ID_SIZE
      val startKey = StorageFormat.getBytes(segment, startKeyOffset, table.keySize)
      val endKeyOffset = startKeyOffset + table.keySize
      val endKey = StorageFormat.getBytes(segment, endKeyOffset, table.keySize)
      NodeBlock.Child(id, startKey, endKey)
    }.toList
  }

  override def put(rows: Seq[Row]): List[NodeBlock] = {
    val newChildren = doPut(rows, children, Nil)
    val blocks = NodeBlock.splitAndWrite(table, newChildren, startKey, endKey).toList
    table.freeBlock(id)
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

        doPut(tailRows, tail, result ++ blocks)

      case Nil => result
    }
  }
}

object NodeBlock {

  case class Child(id: Int, startKey: Array[Byte], endKey: Array[Byte])

  def write(table: KTable, children: Seq[Child], startKey: Array[Byte], endKey: Array[Byte]): Int = {
    val id = table.allocateBlock
    val segment = table.blockSegment(id)
    write(id, segment, children, table.keySize, startKey, endKey)
  }

  def write(
      id: Int,
      blockSegment: MemorySegment,
      children: Seq[Child],
      keySize: Int,
      startKey: Array[Byte],
      endKey: Array[Byte]
  ): Int = {
    Metrics.writeNodeBlock.measure(1) {

      require(
        startKey.length == endKey.length && startKey.length == keySize,
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
          val startKeyOffset = baseOffset + Block.BLOCK_ID_SIZE
          StorageFormat.setBytes(b.startKey, 0, blockSegment, startKeyOffset, keySize)
          val endKeyOffset = startKeyOffset + keySize
          StorageFormat.setBytes(b.endKey, 0, blockSegment, endKeyOffset, keySize)
      }
      id
    }
  }

  def childSize(keySize: Int): Int = {
    Block.BLOCK_ID_SIZE * keySize * 2
  }

  def maxNumOfChildren(keySize: Int): Int = {
    (Block.BLOCK_SIZE - Block.headerSize(keySize)) / NodeBlock.childSize(keySize)
  }

  def fromBlocks(table: KTable, blocks: Seq[NodeBlock], startKey: Array[Byte], endKey: Array[Byte]): NodeBlock = {
    val children = block2children(blocks)
    apply(table, children, startKey, endKey)
  }

  private def block2children(blocks: Seq[Block]) = {
    blocks.map { b =>
      NodeBlock.Child(b.id, b.startKey, b.endKey)
    }
  }

  def apply(table: KTable, children: Seq[NodeBlock.Child], startKey: Array[Byte], endKey: Array[Byte]): NodeBlock = {
    val id = NodeBlock.write(table, children, startKey, endKey)
    new NodeBlock(id, table)
  }

  def splitAndWriteBlocks(
      table: KTable,
      blocks: Seq[Block],
      start: Array[Byte],
      end: Array[Byte]
  ): Seq[NodeBlock] = {
    val children = block2children(blocks)
    splitAndWrite(table, children, start, end)
  }

  def splitAndWrite(
      table: KTable,
      newChildren: Seq[NodeBlock.Child],
      start: Array[Byte],
      end: Array[Byte]
  ): Seq[NodeBlock] = {
    val s = newChildren.size
    if (s <= maxNumOfChildren(start.length)) {
      Seq(NodeBlock(table, newChildren, start, end))
    } else {
      val (left, right) = newChildren.splitAt(s / 2)
      val middle = left.last.endKey
      val incMiddle = StorageFormat.incByteArray(middle, start.length)
      splitAndWrite(table, left, start, middle) ++ splitAndWrite(table, right, incMiddle, end)
    }
  }

}
