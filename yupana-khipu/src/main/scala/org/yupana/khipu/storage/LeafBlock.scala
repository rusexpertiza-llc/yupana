package org.yupana.khipu.storage

import org.yupana.api.schema.Table
import org.yupana.khipu.KhipuMetricCollector.Metrics

import java.lang.foreign.MemorySegment
import scala.collection.AbstractIterator
import scala.collection.compat.immutable.ArraySeq

/**
  * Format:
  *   Int16 - MAGIC
  *   Int32 - size of key
  *   bytes - startKey
  *   bytes - endKey
  *   Int32 - num of records
  *   rows - ???
  */
class LeafBlock(override val id: Int, override val table: KTable) extends Block {

  private val headerSize = Block.headerSize(keySize)

  val payload: MemorySegment = segment.asSlice(Block.headerSize(keySize))

  override def put(puts: Seq[Row]): List[LeafBlock] = {

    require(
      puts.forall(r => rowSize(r) <= KTable.MAX_ROW_SIZE),
      s"One or more row have size more than ${KTable.MAX_ROW_SIZE}"
    )

    val existing = Metrics.load.measure(1) {
      readRows()
    }
    val rows = Metrics.merge.measure(1) {
      mergeRows(existing, puts.toIndexedSeq).toIndexedSeq
    }

    Metrics.splitAndWriteLeafBlocks.measure(1) {
      val bs = splitAndWrite(rows, startKey, endKey).toList
      table.freeBlock(id)
      bs
    }
  }

  private def splitAndWrite(
      rows: IndexedSeq[Row],
      start: MemorySegment,
      end: MemorySegment
  ): Seq[LeafBlock] = {
    if (rows.nonEmpty) {

      val size = rows.foldLeft(0)((s, r) => s + StorageFormat.alignLong(rowSize(r)))

      if (headerSize + size > Block.BLOCK_SIZE) {

        val splitIndex = rows.length / 2

        val (left, right) = rows.splitAt(splitIndex)
        val splitPoint = left.last.key
        val incSplitPoint = StorageFormat.incMemorySegment(splitPoint, keySize)

        splitAndWrite(left, start, splitPoint) ++ splitAndWrite(right, incSplitPoint, end)
      } else {
        Metrics.writeLeafBlock.measure(1) {
          val block = writeBlock(rows.iterator, start, end)
          Seq(block)
        }
      }
    } else {
      Seq.empty
    }
  }

  private def mergeRows(existing: IndexedSeq[Row], puts: IndexedSeq[Row]): Iterator[Row] = {

    new AbstractIterator[Row] {
      var eIdx = 0
      var pIdx = 0

      override def hasNext: Boolean = {
        eIdx < existing.length || pIdx < puts.length
      }

      override def next(): Row = {
        val cmp = if (eIdx < existing.length && pIdx < puts.length) {
          StorageFormat.compareTo(existing(eIdx).key, puts(pIdx).key, keySize)
        } else if (pIdx < puts.length) {
          1
        } else {
          -1
        }

        if (cmp >= 0) {
          if (cmp == 0) {
            eIdx += 1
          }
          val r = puts(pIdx)
          pIdx += 1
          r
        } else {
          val r = existing(eIdx)
          eIdx += 1
          r
        }
      }
    }
  }

  def readRows(): IndexedSeq[Row] = {
    var offset = 0L
    val rows = Array.ofDim[Row](numOfRecords)
    var i = 0
    while (offset < rowsDataSize) {
      val rowSize = StorageFormat.getInt(payload, offset)
      val key = StorageFormat.getBytes(payload, offset + 8, keySize)
      val value = StorageFormat.getBytes(payload, offset + 8 + keySize, rowSize - keySize - 8)
      rows(i) = Row(key, value)
      offset += StorageFormat.alignLong(rowSize)
      i += 1
    }
    ArraySeq.unsafeWrapArray(rows)
  }

  private def writeBlock(rows: Iterator[Row], start: MemorySegment, end: MemorySegment): LeafBlock = {
    val newId = table.allocateBlock
    val dstData = table.blockSegment(newId)
    var dstPos = headerSize
    var count = 0

    rows.foreach { row =>
      val size = rowSize(row)
      val offset = StorageFormat.alignLong(size)
      StorageFormat.setInt(size, dstData, dstPos)
      StorageFormat.copy(row.key, dstData, dstPos + 8)
      StorageFormat.copy(row.value, dstData, dstPos + 8 + keySize)
      dstPos += offset
      count += 1
    }

    Block.writeHeader(
      table.blockSegment(newId),
      newId,
      Block.LEAF_KIND,
      keySize,
      start,
      end,
      count,
      dstPos - headerSize
    )
    new LeafBlock(newId, table)
  }

  private def rowSize(row: Row) = {
    (8 + row.key.byteSize() + row.value.byteSize()).toInt
  }
}

object LeafBlock {

  def initEmptyBlock(id: Int, blockSegment: MemorySegment, table: Table): NodeBlock.Child = {
    val keySize = StorageFormat.keySize(table)
    val startKey = StorageFormat.fromBytes(Array.ofDim[Byte](keySize))
    val endKey = StorageFormat.fromBytes(Array.fill[Byte](keySize)(0xFF.toByte))

    Block.writeHeader(
      blockSegment,
      id,
      Block.LEAF_KIND,
      keySize,
      startKey,
      endKey,
      0,
      0
    )
    NodeBlock.Child(id, startKey, endKey)
  }

}
