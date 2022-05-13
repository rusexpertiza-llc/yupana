package org.yupana.khipu

import jdk.incubator.foreign.MemorySegment

class BTree(segment: MemorySegment, root: NodeBlock) {

  def put(rows: Seq[Row]): BTree = {
    val newRoot = root.put(rows) match {
      case b :: Nil => b
      case bs       => new NodeBlock(segment, bs)
    }

    new BTree(segment, newRoot)
  }

  def getLeafNodes: Seq[LeafBlock] = ???
}
