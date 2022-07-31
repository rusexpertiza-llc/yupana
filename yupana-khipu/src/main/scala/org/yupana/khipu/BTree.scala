package org.yupana.khipu

class BTree(root: NodeBlock, table: KTable) {

  def explain(): Unit = {
    println("ROOT children size:" + root.children.size)

    var minLevel = Int.MaxValue
    var maxLevel = 0
    var minRows = Int.MaxValue
    var maxRows = 0

    def next(node: Block, level: Int): Unit = {
      minLevel = math.min(level, minLevel)
      maxLevel = math.max(level, maxLevel)
      node match {
        case n: NodeBlock =>
          n.children.foreach(n => next(table.block(n.id), level + 1))
        case l: LeafBlock =>
          minRows = math.min(l.numOfRecords, minRows)
          maxRows = math.max(l.numOfRecords, maxRows)
      }
    }

    next(root, 1)

    println("MIN LEVEL: " + minLevel)
    println("MAX LEVEL: " + maxLevel)

    println("MIN ROWS: " + minRows)
    println("MAX ROWS: " + maxRows)

  }

  val rootId: Int = root.id

  private val rowsOrdering = Ordering.comparatorToOrdering(StorageFormat.BYTES_COMPARATOR)

  def nodeIds: Seq[Int] = {

    def loop(block: Block): Seq[Int] = {
      block match {
        case b: NodeBlock =>
          b.id +: b.children.flatMap(c => loop(table.block(c.id)))
        case l: LeafBlock =>
          Seq(l.id)
      }
    }

    loop(root)
  }

  def put(rows: Seq[Row]): BTree = {
    val newTopLevelBlocks = root.put(rows.sortBy(_.key)(rowsOrdering))

    def loop(bs: Seq[NodeBlock]): Seq[NodeBlock] = {
      if (bs.size == 1) {
        bs
      } else {
        val s = NodeBlock.splitAndWriteBlocks(table, bs, root.startKey, root.endKey)
        loop(s)
      }
    }

    val newRootSeq = loop(newTopLevelBlocks)

    // val newRootSeq =  NodeBlock.splitAndWriteBlocks(table, newTopLevelBlocks, root.startKey, root.endKey)
    require(newRootSeq.size == 1, "Something went wrong, tree construction returns more then  one root block")
    new BTree(newRootSeq.head, table)
  }

  def getLeafBlocks: Seq[LeafBlock] = {
    def loop(block: Block): Seq[LeafBlock] = {
      block match {
        case b: NodeBlock =>
          b.children.flatMap(c => loop(table.block(c.id)))
        case l: LeafBlock =>
          Seq(l)
      }
    }
    loop(root)
  }

  def getNodeBlocks: Seq[NodeBlock] = {
    def loop(block: Block): Seq[NodeBlock] = {
      block match {
        case b: NodeBlock =>
          Seq(b) ++ b.children.flatMap(c => loop(table.block(c.id)))
        case l: LeafBlock =>
          Seq.empty
      }
    }
    loop(root)
  }

}

object BTree {
  def apply(rootId: Int, table: KTable): BTree = {
    val root = new NodeBlock(rootId, table)
    new BTree(root, table)
  }
}
