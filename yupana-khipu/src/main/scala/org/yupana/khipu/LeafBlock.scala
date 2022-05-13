package org.yupana.khipu

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

  private val rowsOrdering = Ordering.comparatorToOrdering(StorageFormat.BYTES_COMPARATOR)

  override def put(rows: Seq[Row]): List[Block] = {

    require(
      rows.forall(r => rowSize(r) <= LeafBlock.MAX_ROW_SIZE),
      s"One or more row have size more than ${LeafBlock.MAX_ROW_SIZE}"
    )
    merge(rows).toList

  }

  private def merge(rows: Seq[Row]) = {

    var dstBlocks = Seq.empty[LeafBlock]
    var dstPos = 0L

    val sorted = sortRows(rows).toArray
    var srcPos = 0L
    var putRowIdx = 0

    val srcRowBytes = Array.ofDim[Byte](LeafBlock.MAX_ROW_SIZE)

    val headerSize = Block.headerSize(keySize)

    while (putRowIdx < sorted.length || srcPos < rowsDataSize) {

      val newId = table.allocateBlock
      val dstData = table.blockSegment(newId)
      dstPos = Block.headerSize(keySize)
      val startKeyPos = dstPos
      var endKeyPos = startKeyPos
      var recordsCount = 0
      var payloadSize = 0

      while (dstPos < dstData.byteSize() && putRowIdx < sorted.length || srcPos < rowsDataSize) {

        val srcRowSize = StorageFormat.getInt(segment, headerSize + srcPos)
        if (srcPos < rowsDataSize) {
          StorageFormat.getBytes(segment, headerSize + srcPos + 4, srcRowBytes, 0, srcRowSize - 4)
        }

        val c = if (putRowIdx < sorted.length && srcPos < rowsDataSize) {
          val putRow = sorted(putRowIdx)
          StorageFormat.compareTo(srcRowBytes, 4, putRow.key, 0, keySize)
        } else if (srcPos < rowsDataSize) {
          -1
        } else {
          1
        }

        val offset = if (c >= 0) {
          val putRow = sorted(putRowIdx)
          rowSize(putRow)
        } else {
          srcRowSize
        }

        if (dstPos + offset < dstData.byteSize()) {

          if (c >= 0) {
            if (c == 0) {
              // replace row else insert
              srcPos += srcRowSize
            }
            val putRow = sorted(putRowIdx)
            StorageFormat.setInt(offset, dstData, dstPos)
            StorageFormat.setBytes(putRow.key, 0, dstData, dstPos + 4, keySize)
            StorageFormat.setBytes(putRow.value, 0, dstData, dstPos + 4 + keySize, putRow.value.length)
            putRowIdx += 1
          } else {
            StorageFormat.setInt(srcRowSize, dstData, dstPos)
            StorageFormat.setBytes(srcRowBytes, 0, dstData, dstPos + 4, srcRowSize - 4)
            srcPos += srcRowSize
          }
          recordsCount += 1
          payloadSize += offset
        }
        endKeyPos = dstPos
        dstPos += offset
      }

      Block.writeHeader(
        segment,
        newId,
        Block.LEAF_KIND,
        keySize,
        StorageFormat.getBytes(dstData, startKeyPos + 4, keySize),
        StorageFormat.getBytes(dstData, endKeyPos + 4, keySize),
        recordsCount,
        payloadSize
      )

      val b = new LeafBlock(newId, table)
      dstBlocks = dstBlocks :+ b
    }
    dstBlocks
  }

  private def rowSize(row: Row) = {
    4 + row.key.length + row.value.length
  }

  private def sortRows(rows: Seq[Row]) = {
    rows.sortBy(_.key)(rowsOrdering)
  }
}

object LeafBlock {
  val MAX_ROW_SIZE = 200
}
