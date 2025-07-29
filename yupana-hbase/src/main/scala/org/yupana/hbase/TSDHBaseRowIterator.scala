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

package org.yupana.hbase

import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.hbase.client.{ Result => HBaseResult }
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{ Cell, CellUtil }
import org.yupana.api.Time
import org.yupana.api.schema.{ DictionaryDimension, HashDimension, RawDimension, Table }
import org.yupana.api.types.{ ByteReaderWriter, DataType }
import org.yupana.core.model.{ BatchDataset, DatasetSchema }
import org.yupana.hbase.HBaseUtils.TAGS_POSITION_IN_ROW_KEY
import org.yupana.serialization.{ MemoryBuffer, MemoryBufferEvalReaderWriter }

import scala.collection.AbstractIterator

class TSDHBaseRowIterator(
    context: InternalQueryContext,
    rows: Iterator[HBaseResult],
    schema: DatasetSchema
) extends AbstractIterator[BatchDataset]
    with StrictLogging {

  implicit val readerWriter: ByteReaderWriter[MemoryBuffer] = MemoryBufferEvalReaderWriter

  private val dimensions = context.table.dimensionSeq.toArray

  private val maxFamiliesCount = context.table.metrics.map(_.group).distinct.size

  private val offsets = Array.ofDim[Int](maxFamiliesCount)
  private val endOffsets = Array.ofDim[Int](maxFamiliesCount)

  private var cells = Array.empty[Cell]
  private var familiesCount = 0
  private var currentTime = Long.MaxValue
  private var currentRowKey = Array.empty[Byte]

  override def hasNext: Boolean = {
    rows.hasNext || currentTime != Long.MaxValue
  }

  override def next(): BatchDataset = {
    if (rows.isEmpty && currentTime == Long.MaxValue) {
      throw new IllegalStateException("Next on empty iterator")
    }

    logger.info("WTF CREATING NEW BATCH DATASET")
    val batch = new BatchDataset(schema)
    var rowNum = 0
    while (rowNum < batch.capacity && (rows.hasNext || currentTime != Long.MaxValue)) {

      if (rows.hasNext && currentTime == Long.MaxValue) {
        nextHBaseRow()
      }

      loadNextDataPoint(batch, rowNum)
      rowNum += 1
    }
    batch
  }

  private def nextHBaseRow(): Unit = {
    logger.info("WTF CALLING .next ON ROWS ITERATOR")
    val result = rows.next()
    currentRowKey = result.getRow
    cells = result.rawCells()
    familiesCount = fillFamiliesOffsets(cells)

    def findMinTime(): Long = {
      var min = Long.MaxValue
      var i = 0
      while (i < familiesCount) {
        min = math.min(min, getTimeOffset(cells(offsets(i))))
        i += 1
      }
      min
    }

    currentTime = findMinTime()
  }

  private def loadNextDataPoint(batchDataset: BatchDataset, rowNum: Int): Unit = {
    loadRow(currentRowKey, batchDataset, rowNum)
    var nextMinTime = Long.MaxValue
    var i = 0
    while (i < familiesCount) {
      val offset = offsets(i)
      val cell = cells(offset)
      if (getTimeOffset(cell) == currentTime) {
        loadCell(cell, batchDataset, rowNum)
        if (offset < endOffsets(i)) {
          nextMinTime = math.min(nextMinTime, getTimeOffset(cells(offset + 1)))
          offsets(i) = offset + 1
        }
      }
      i += 1
    }
    currentTime = nextMinTime
  }

  private def loadRow(rowKey: Array[Byte], batch: BatchDataset, rowNum: Int): Unit = {
    val baseTime = Bytes.toLong(rowKey)
    batch.set(rowNum, Time(baseTime + currentTime))
    var i = 0
    val bb = MemoryBuffer.ofBytes(rowKey).asSlice(TAGS_POSITION_IN_ROW_KEY, rowKey.length - TAGS_POSITION_IN_ROW_KEY)
    dimensions.foreach { dim =>
      val pos = bb.position()

      dim match {
        case rd: RawDimension[_] =>
          val value = rd.rStorable.read(bb)
          batch.set(rowNum, (Table.DIM_TAG_OFFSET + i).toByte, value)(rd.dataType.internalStorable)
        case _ =>
          dim.rStorable.read(bb)
      }
      if (dim.isInstanceOf[RawDimension[_]]) {}
      if (schema.needId((Table.DIM_TAG_OFFSET + i).toByte)) {
        val bytes = new Array[Byte](dim.rStorable.size)
        bb.get(pos, bytes)
        batch.setId(rowNum, (Table.DIM_TAG_OFFSET + i).toByte, new String(Hex.encodeHex(bytes)))
      }

      i += 1
    }
  }

  private def loadCell(cell: Cell, batchDataset: BatchDataset, rowNum: Int): Boolean = {
    val bb = MemoryBuffer.ofBytes(cell.getValueArray).asSlice(cell.getValueOffset, cell.getValueLength)
    var correct = true
    while (bb.hasRemaining() && correct) {
      val tag = bb.get()
      context.table.fieldForTag(tag) match {
        case Some(Left(metric)) =>
          val v = metric.dataType.storable.read(bb)
          batchDataset.set(rowNum, tag, v)(metric.dataType.internalStorable)
        case Some(Right(_: DictionaryDimension)) =>
          val v = DataType.stringDt.storable.read(bb)
          batchDataset.set(rowNum, tag, v)
        case Some(Right(hd: HashDimension[_, _])) =>
          val v = hd.tStorable.read(bb)
          batchDataset.set(rowNum, tag, v)(hd.dataType.internalStorable)
        case _ =>
          logger.warn(s"Unknown tag: $tag, in table: ${context.table.name}")
          correct = false
      }
    }
    correct
  }

  private def fillFamiliesOffsets(cells: Array[Cell]): Int = {
    var i = 1
    var j = 1
    var prevFamilyCell = cells(0)
    offsets(0) = 0
    while (i < cells.length) {
      val cell = cells(i)
      endOffsets(j - 1) = i - 1
      if (!CellUtil.matchingFamily(prevFamilyCell, cell)) {
        prevFamilyCell = cell
        offsets(j) = i
        j += 1
      }
      i += 1
    }
    endOffsets(j - 1) = cells.length - 1

    j
  }

  private def getTimeOffset(cell: Cell): Long = {
    Bytes.toLong(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
  }
}
