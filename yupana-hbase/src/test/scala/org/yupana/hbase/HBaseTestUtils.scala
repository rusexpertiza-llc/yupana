package org.yupana.hbase

import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.yupana.api.types.DataType

import scala.collection.JavaConverters._

object HBaseTestUtils {

  def row(baseTime: Long, dims: Any*) = {
    val key = Bytes.toBytes(baseTime) ++ dims.foldLeft(Array.ofDim[Byte](0)) { (a, d) =>
      val dimBytes = d match {
        case x: Long  => Bytes.toBytes(x)
        case x: Short => Bytes.toBytes(x)
        case x: Byte  => Bytes.toBytes(x)
        case x: Int   => Bytes.toBytes(x)
      }
      a ++ dimBytes
    }
    new RowBuilder(key, Nil)
  }

  class RowBuilder(key: Array[Byte], cells: List[(String, Long, Array[Byte])]) {

    def cell(family: String, time: Long): RowBuilder = {
      new RowBuilder(key, (family, time, Array.empty[Byte]) :: cells)
    }

    def field(tag: Int, value: Long): RowBuilder = {
      field(tag, DataType.intDt[Long].storable.write(value))
    }

    def field(tag: Int, value: String): RowBuilder = {
      field(tag, DataType.stringDt.storable.write(value))
    }

    def field(tag: Int, value: Double): RowBuilder = {
      field(tag, DataType.fracDt[Double].storable.write(value))
    }

    def field(tag: Int, value: BigDecimal): RowBuilder = {
      field(tag, DataType.fracDt[BigDecimal].storable.write(value))
    }

    def field(tag: Int, value: Array[Byte]): RowBuilder = {
      val f = tag.toByte +: value
      val (family, time, bytes) = cells.head
      new RowBuilder(key, (family, time, bytes ++ f) :: cells.tail)
    }

    def hbaseRow: Result = {
      val cs: List[Cell] = cells.reverse.map {
        case (family, time, value) =>
          TestCell(
            key,
            Bytes.toBytes(family),
            Bytes.toBytes(time),
            value
          )
      }

      Result.create(cs.asJava)
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
