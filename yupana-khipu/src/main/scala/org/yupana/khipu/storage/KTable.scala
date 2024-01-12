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

import org.yupana.api.schema.Table
import org.yupana.api.utils.SortedSetIterator
import org.yupana.khipu.KhipuMetricCollector.Metrics
import org.yupana.khipu.storage.KTable._

import java.io.RandomAccessFile
import java.lang.foreign.{ Arena, MemorySegment }
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.{ Path, StandardOpenOption }
import scala.collection.immutable.Queue

trait KTable {

  def table: Table
  def allocatePayloadSegment(idx: Int): MemorySegment
  def headerSegment: MemorySegment

  var payloadSegments: Vector[Option[MemorySegment]] = Vector.empty

  val keySize: Int = StorageFormat.keySize(table)

  initTable()
  require(StorageFormat.getShort(headerSegment, 0) == KTable.MAGIC, "Wrong table format, invalid magic")

  def rootId: Int = StorageFormat.getInt(headerSegment, KTable.ROOT_ID_OFFSET)

  private def setRootId(id: Int): Unit = StorageFormat.setInt(id, headerSegment, KTable.ROOT_ID_OFFSET)

  private var bTree = BTree(rootId, this)

  private var freeBlocks: Queue[Int] = findFreeBlocks(bTree)

  private var maxBlockId = getAllocatedBlocks(bTree).max

  private def getAllocatedBlocks(tree: BTree): Seq[Int] = {
    tree.nodeIds
  }

  private def findFreeBlocks(tree: BTree): Queue[Int] = {
    Metrics.findFreeBlock.measure(1) {
      val allocated = getAllocatedBlocks(tree).toSet
      val maxId = allocated.max
      val free = (0 to maxId).filterNot(allocated.contains)
      Queue.from(free)
    }
  }

  def leafBlocks: Iterator[LeafBlock] = {
    bTree.getLeafBlocks
  }
  def nodeBlocks: Seq[NodeBlock] = {
    bTree.getNodeBlocks
  }

  def scan(prefixes: SortedSetIterator[Prefix]): Cursor = {
    new Cursor(this, bTree, prefixes)
  }

  def scan(): Cursor = {
    new Cursor(this, bTree, SortedSetIterator.empty)
  }

  def put(row: Row): Unit = {
    put(Seq(row))
  }

  def put(rows: Seq[Row]): Unit = {
    Metrics.put.measure(1) {
      bTree = bTree.put(rows)
//      freeBlocks = findFreeBlocks(bTree)
      setRootId(bTree.rootId)
    }
//    println("LeafBlocks " + bTree.getLeafBlocks.size)
//    println("ModeBlocks " + bTree.getNodeBlocks.size)
  }

  def explain(): Unit = {
    bTree.explain()

  }

  def blockSegment(id: Int): MemorySegment = {
    val blocksInSegment = KTable.SEGMENT_SIZE / Block.BLOCK_SIZE
    val segmentIdx = id / blocksInSegment
    val s = segment(segmentIdx)
    val offset = (id - segmentIdx * blocksInSegment) * Block.BLOCK_SIZE
    s.asSlice(offset, Block.BLOCK_SIZE)
  }

  def allocateBlock: Int = {
    Metrics.allocateBlock.measure(1) {
      val (bockId, newFreeBlocks) = freeBlocks.dequeueOption match {
        case Some(r) =>
          r
        case None =>
          maxBlockId += 1
          (maxBlockId, freeBlocks)
      }

      freeBlocks = newFreeBlocks
      bockId
    }
  }

  def freeBlock(id: Int): Unit = {
    freeBlocks = freeBlocks.enqueue(id)
  }

  def block(id: Int): Block = {
    Block.checkFormatAndGetBlockKind(id, this) match {
      case Block.NODE_KIND => new NodeBlock(id, this)
      case Block.LEAF_KIND => new LeafBlock(id, this)
      case u               => throw new IllegalStateException(s"Unknown block $u")
    }
  }

  def segment(idx: Int): MemorySegment = {
    if (idx < payloadSegments.size) {
      payloadSegments(idx) match {
        case Some(s) =>
          s
        case None =>
          val s = allocatePayloadSegment(idx)
          payloadSegments = payloadSegments.updated(idx, Some(s))
          s
      }
    } else {
      val s = allocatePayloadSegment(idx)
      payloadSegments = payloadSegments.padTo(idx + 1, None).updated(idx, Some(s))
      s
    }
  }

  private def initTable(): Unit = {
    if (StorageFormat.getShort(headerSegment, 0) != MAGIC) {
      StorageFormat.setShort(MAGIC, headerSegment, 0)
      StorageFormat.setShort(0, headerSegment, ROOT_ID_OFFSET)
      val child = LeafBlock.initEmptyBlock(1, blockSegment(1), table)
      NodeBlock.write(0, blockSegment(0), Seq(child), StorageFormat.keySize(table), child.startKey, child.endKey)
    }
  }
}

class HeapKTable(val table: Table) extends KTable {

  override def allocatePayloadSegment(idx: Int): MemorySegment = {
    StorageFormat.allocateNative(SEGMENT_SIZE)

  }

  override lazy val headerSegment: MemorySegment = {
    StorageFormat.allocateNative(HEADER_SIZE)
  }
}

class MMapFileKTable(path: Path, val table: Table) extends KTable {

  override def allocatePayloadSegment(idx: Int): MemorySegment = {
    checkOrCreateFile
    val offset = HEADER_SEGMENT_SIZE + idx.toLong * SEGMENT_SIZE
    val ch = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)
    ch.map(MapMode.READ_WRITE, offset, SEGMENT_SIZE, Arena.global())
  }

  override lazy val headerSegment: MemorySegment = {
    checkOrCreateFile
    val ch = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)
    ch.map(MapMode.READ_WRITE, 0, HEADER_SEGMENT_SIZE, Arena.global())
  }

  private def checkOrCreateFile = {
    if (!path.toFile.exists()) {
      new RandomAccessFile(path.toFile, "rw")
    }
  }

}

object KTable {
  val MAGIC = 123.toShort
  val HEADER_SIZE = 4 + Block.BLOCK_ID_SIZE

  val ROOT_ID_OFFSET = 4
  val SEGMENT_SIZE = 128 * 1024 * 1024

  val HEADER_SEGMENT_SIZE = 4096

  val MAX_ROW_SIZE = 2000

  def allocateHeap(table: Table): KTable = {
    new HeapKTable(table)
  }

  def mapFile(path: Path, table: Table): KTable = {
    new MMapFileKTable(path, table)
  }

}
