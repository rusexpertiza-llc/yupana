package org.yupana.khipu

import jdk.incubator.foreign.MemorySegment
import org.yupana.api.schema.Table
import org.yupana.khipu.KhipuMetricCollector.Metrics

import scala.collection.AbstractIterator

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
      puts.forall(r => rowSize(r) <= LeafBlock.MAX_ROW_SIZE),
      s"One or more row have size more than ${LeafBlock.MAX_ROW_SIZE}"
    )

    val existing = Metrics.load.measure(1) {
      loadRows()
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
      start: Array[Byte],
      end: Array[Byte]
  ): Seq[LeafBlock] = {
    if (rows.nonEmpty) {

      val size = rows.foldLeft(0)((s, r) => s + rowSize(r))

      if (headerSize + size > Block.BLOCK_SIZE) {

        val splitIndex = rows.length / 2

        val (left, right) = rows.splitAt(splitIndex)
        val splitPoint = left.last.key
        val incSplitPoint = StorageFormat.incByteArray(splitPoint, keySize)

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

  private def loadRows(): IndexedSeq[Row] = {
    var offset = 0L
    val rows = Array.ofDim[Row](numOfRecords)
    var i = 0
    while (offset < rowsDataSize) {
      val srcRowSize = StorageFormat.getInt(payload, offset)
      val key = StorageFormat.getBytes(payload, offset + 4, keySize)
      val value = StorageFormat.getBytes(payload, offset + 4 + keySize, srcRowSize - keySize - 4)
      rows(i) = Row(key, value)
      offset += srcRowSize
      i += 1
    }
    rows
  }

  private def writeBlock(rows: Iterator[Row], start: Array[Byte], end: Array[Byte]): LeafBlock = {
    val newId = table.allocateBlock
    val dstData = table.blockSegment(newId)
    var dstPos = Block.headerSize(keySize)
    var count = 0

    rows.foreach { row =>
      val offset = rowSize(row)
      StorageFormat.setInt(offset, dstData, dstPos)
      StorageFormat.setBytes(row.key, 0, dstData, dstPos + 4, keySize)
      StorageFormat.setBytes(row.value, 0, dstData, dstPos + 4 + keySize, row.value.length)
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
    4 + row.key.length + row.value.length
  }
}

object LeafBlock {

  def initEmptyBlock(id: Int, blockSegment: MemorySegment, table: Table): NodeBlock.Child = {
    val keySize = StorageFormat.keySize(table)
    val startKey = Array.ofDim[Byte](keySize)
    val endKey = Array.fill[Byte](keySize)(0xFF.toByte)

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

  val MAX_ROW_SIZE = 200
}
