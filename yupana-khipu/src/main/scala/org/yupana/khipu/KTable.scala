package org.yupana.khipu

import jdk.incubator.foreign.MemorySegment
import org.yupana.api.schema.Table

class KTable(segment: MemorySegment, val table: Table) {

  val keySize: Int = StorageFormat.keySize(table)
  val headerSize: Int = Block.headerSize(keySize)

  private var bTree = new BTree(segment)

  def scan(): Cursor = new Cursor(table, bTree.getLeafNodes, None)

  def put(row: Row): Unit = {
    put(Seq(row))
  }

  def put(rows: Seq[Row]): Unit = {
    bTree = bTree.put(rows)
  }

  def blockSegment(id: Int): MemorySegment = ???

  def allocateBlock: Int = ???

  def block(id: Int): Block = {
    Block.checkFormatAndGetBlockKind(id, this) match {
      case Block.NODE_KIND => new NodeBlock(id, this)
      case Block.LEAF_KIND => new LeafBlock(id, this)
    }
  }
}

object KTable {

  def empty(table: Table): KTable = {
    val s = MemorySegment.ofArray(Array.ofDim[Byte](10))
    new KTable(s, table)
  }

}
