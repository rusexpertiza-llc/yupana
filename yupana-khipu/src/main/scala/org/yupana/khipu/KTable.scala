package org.yupana.khipu

import jdk.incubator.foreign.{ MemorySegment, ResourceScope }
import org.yupana.api.schema.Table

import java.nio.channels.FileChannel.MapMode
import java.nio.file.Path
import KhipuMetricCollector.Metrics
import org.yupana.khipu.KTable.{ HEADER_SEGMENT_SIZE, HEADER_SIZE, MAGIC, ROOT_ID_OFFSET, SEGMENT_SIZE }

import java.io.RandomAccessFile
import scala.collection.immutable.Queue

trait KTable {

  def table: Table
  def allocatePayloadSegment(idx: Int): MemorySegment
  def headerSegment: MemorySegment

  var payloadSegments: Vector[Option[MemorySegment]] = Vector.empty

  val keySize: Int = StorageFormat.keySize(table)
  val headerSize: Int = Block.headerSize(keySize)

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

  def getLeafBlocks(): Seq[LeafBlock] = {
    bTree.getLeafBlocks
  }
  def getNodeBlocks(): Seq[NodeBlock] = {
    bTree.getNodeBlocks
  }

  def scan(): Cursor = new Cursor(table, bTree.getLeafBlocks, None)

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
    val a = Array.ofDim[Byte](SEGMENT_SIZE)
    MemorySegment.ofArray(a)
  }

  override lazy val headerSegment: MemorySegment = {
    val a = Array.ofDim[Byte](HEADER_SIZE)
    MemorySegment.ofArray(a)
  }
}

class MMapFileKTable(path: Path, val table: Table) extends KTable {

  private lazy val scope = ResourceScope.newConfinedScope()

  override def allocatePayloadSegment(idx: Int): MemorySegment = {
    checkOrCreateFile
    val offset = HEADER_SEGMENT_SIZE + idx.toLong * SEGMENT_SIZE
    MemorySegment.mapFile(path, offset, SEGMENT_SIZE, MapMode.READ_WRITE, scope)
  }

  override lazy val headerSegment: MemorySegment = {
    checkOrCreateFile
    MemorySegment.mapFile(path, 0, HEADER_SEGMENT_SIZE, MapMode.READ_WRITE, scope)
  }

  private def checkOrCreateFile = {
    if (!path.toFile.exists()) {
      new RandomAccessFile(path.toFile, "rw")
    }
  }

}

object KTable {
  val MAGIC = 123.toShort
  val MAGIC_SIZE = 2
  val HEADER_SIZE = MAGIC_SIZE + Block.BLOCK_ID_SIZE

  val ROOT_ID_OFFSET = MAGIC_SIZE
  val PAYLOAD_OFFSET = HEADER_SIZE
  val SEGMENT_SIZE = 128 * 1024 * 1024

  val HEADER_SEGMENT_SIZE = 4096

  def allocateHeap(table: Table): KTable = {
    new HeapKTable(table)
  }

  def mapFile(path: Path, table: Table): KTable = {
    new MMapFileKTable(path, table)
  }

}
