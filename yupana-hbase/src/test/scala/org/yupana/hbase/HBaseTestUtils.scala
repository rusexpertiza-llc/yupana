package org.yupana.hbase

import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.yupana.api.types.DataType

import scala.collection.JavaConverters._

object HBaseTestUtils {

  def row(key: TSDRowKey[Long]) = new RowBuilder(key, Nil)

  class RowBuilder(key: TSDRowKey[Long], cells: List[Cell]) {

    def cell(time: Long, tag: Int, value: Long): RowBuilder = {
      cell(time, tag, DataType.intDt[Long].writable.write(value))
    }

    def cell(time: Long, tag: Int, value: String): RowBuilder = {
      cell(time, tag, DataType.stringDt.writable.write(value))
    }

    def cell(time: Long, tag: Int, value: Double): RowBuilder = {
      cell(time, tag, DataType.fracDt[Double].writable.write(value))
    }

    def cell(time: Long, tag: Int, value: Array[Byte]): RowBuilder = {

      val cell = new TestCell(
        HBaseUtils.rowKeyToBytes(key),
        Bytes.toBytes("t"),
        Array(tag.toByte) ++ Bytes.toBytes(time),
        value
      )

      new RowBuilder(key, cells :+ cell)
    }

    def hbaseRow: Result = {
      Result.create(cells.asJava)
    }
  }

  case class TestCell(row: Array[Byte], family: Array[Byte], qualifier: Array[Byte], value: Array[Byte]) extends Cell {
    override def getRowArray: Array[Byte] = row
    override def getRowOffset = 0
    override def getRowLength: Short = row.length.toShort
    override def getFamilyArray: Array[Byte] = family
    override def getFamilyOffset = 0
    override def getFamilyLength = family.length.toByte
    override def getQualifierArray: Array[Byte] = qualifier
    override def getQualifierOffset = 0
    override def getQualifierLength = qualifier.length
    override def getTimestamp = 0L
    override def getTypeByte = 0
    override def getMvccVersion = 0L
    override def getValueArray: Array[Byte] = value
    override def getValueOffset = 0
    override def getValueLength = value.length
    override def getTagsArray: Array[Byte] = null
    override def getTagsOffset = 0
    override def getValue: Array[Byte] = null
    override def getFamily: Array[Byte] = null
    override def getQualifier: Array[Byte] = null
    override def getRow: Array[Byte] = null
    override def getSequenceId = 0L
    override def getTagsLength = 0
  }
}
