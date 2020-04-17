package org.yupana.hbase

import org.apache.hadoop.hbase.{ Cell, CellUtil }
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.yupana.api.schema.Table
import org.yupana.api.types.DataType

import scala.collection.AbstractIterator

class TSDHBaseRow(context: InternalQueryContext, hrow: Result) {
  val key = HBaseUtils.parseRowKey(hrow.getRow, context.table)

  val startOffsets = Array.fill[Int](Table.MAX_TAGS)(-1)

  hrow.rawCells().foldLeft(0) { (index, cell) =>
    val tag = cell.getQualifierArray()(cell.getQualifierOffset)
    if (startOffsets(tag & 0xFF) == -1) startOffsets(tag & 0xFF) = index
    index + 1
  }

  def iterator(mutableData: Array[Option[Any]]) = new AbstractIterator[(Long, Array[Option[Any]])] {

    val currentOffsets = Array.ofDim[Int](Table.MAX_TAGS)

    Array.copy(startOffsets, 0, currentOffsets, 0, startOffsets.length)

    val tagsIdx = currentOffsets.zipWithIndex.filter(_._1 != -1).map(_._2)
    var nextTime = getTime(hrow.rawCells()(currentOffsets(tagsIdx.head)))
    var currentTime = nextTime

    var loaded = false
    var hasNextVal = false
    var first = true

    @inline
    def getTime(cell: Cell) = {
      Bytes.toLong(cell.getQualifierArray, cell.getQualifierOffset + 1)
    }

    override def hasNext: Boolean = {
      if (loaded) {
        true
      } else {
        val r = hasNextVal || (first && tagsIdx.length > 0)
        hasNextVal = if (hasNextVal || first) load else false
        r
      }
    }

    override def next(): (Long, Array[Option[Any]]) = {
      if (hasNext) {
        loaded = false
        first = false
        (key.baseTime + currentTime, mutableData)
      } else {
        throw new IllegalStateException("Next on empty iterator")
      }
    }

    private def load = {
      var minTime = Long.MaxValue
      var res = false
      currentTime = nextTime
      loaded = true

      var i = 0
      while (i < tagsIdx.length) {
        val idx = tagsIdx(i)
        val offset = currentOffsets(idx)
        val cell = hrow.rawCells()(offset)
        val cellTime = getTime(cell)

        context.fieldForTag(idx.toByte) match {
          case Some(Left(metric)) if cellTime == currentTime =>
            val v = metric.dataType.readable.read(CellUtil.cloneValue(cell))
            mutableData(idx) = Some(v)
          case Some(Right(_)) if cellTime == currentTime =>
            val v = DataType.stringDt.readable.read(CellUtil.cloneValue(cell))
            mutableData(idx) = Some(v)
          case _ =>
            mutableData(idx) = None
        }

        if (hrow.rawCells().length > offset + 1 &&
            (i + 1 < tagsIdx.length && offset + 1 < startOffsets(tagsIdx(i + 1)))) {
          val nextTime = getTime(hrow.rawCells()(offset + 1))
          if (nextTime < minTime) minTime = nextTime
        }
        i += 1
      }

      var j = 0
      while (j < tagsIdx.length) {
        val idx = tagsIdx(j)
        val offset = currentOffsets(idx)
        if (hrow.rawCells().length > offset + 1 &&
            (j + 1 < tagsIdx.length && offset + 1 < startOffsets(tagsIdx(j + 1)))) {

          val cell = hrow.rawCells()(offset + 1)
          val cellTime = getTime(cell)
          if (cellTime == minTime) {
            currentOffsets(idx) = offset + 1
            res = true
          }
        }
        j += 1
      }

      nextTime = minTime
      res && minTime != Long.MaxValue
    }
  }

}
