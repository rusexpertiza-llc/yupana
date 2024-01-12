/*
 * Copyright 2019 Rusexpertiza LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yupana.khipu.storage

class BTree(val root: NodeBlock, table: KTable) {

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
        case u => throw new IllegalStateException(s"Unknown block $u")
      }
    }

    next(root, 1)

    println("MIN LEVEL: " + minLevel)
    println("MAX LEVEL: " + maxLevel)

    println("MIN ROWS: " + minRows)
    println("MAX ROWS: " + maxRows)

  }

  val rootId: Int = root.id

  private val rowsOrdering = Ordering.comparatorToOrdering(StorageFormat.ROWS_COMPARATOR)

  def nodeIds: Seq[Int] = {

    def loop(block: Block): Seq[Int] = {
      block match {
        case b: NodeBlock => b.id +: b.children.toList.flatMap(c => loop(table.block(c.id)))
        case l: LeafBlock => Seq(l.id)
        case u            => throw new IllegalStateException(s"Unknown block $u")
      }
    }

    loop(root)
  }

  def put(rows: Seq[Row]): BTree = {
    val sorted = rows.sorted(rowsOrdering)
    val newTopLevelBlocks = root.put(sorted)

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
    require(newRootSeq.size == 1, "Something went wrong, tree have more then one root block")
    new BTree(newRootSeq.head, table)
  }

  def getLeafBlocks: Iterator[LeafBlock] = {
    def loop(block: Block): Iterator[LeafBlock] = {
      block match {
        case b: NodeBlock => b.children.iterator.flatMap(c => loop(table.block(c.id)))
        case l: LeafBlock => Iterator(l)
        case u            => throw new IllegalStateException(s"Unknown block $u")
      }
    }
    loop(root)
  }

  def getNodeBlocks: Seq[NodeBlock] = {
    def loop(block: Block): Seq[NodeBlock] = {
      block match {
        case b: NodeBlock =>
          Seq(b) ++ b.children.flatMap(c => loop(table.block(c.id)))
        case _ =>
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
