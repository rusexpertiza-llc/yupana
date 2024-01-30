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

package org.yupana.core.model

import org.yupana.api.Time
import org.yupana.api.query.{ DimensionExpr, DimensionIdExpr, Expression, MetricExpr, TimeExpr }
import org.yupana.api.schema.{ Dimension, Table }
import org.yupana.api.types.{ ID, InternalStorable, ReaderWriter, TypedInt }
import org.yupana.core.QueryContext
import org.yupana.readerwriter.{ MemoryBuffer, MemoryBufferEvalReaderWriter }

import java.io.ObjectInputStream

class InternalRow(val bytes: MemoryBuffer, val refs: Map[Int, AnyRef]) extends Serializable {

  import InternalRowBuilder.readerWriter

  def isDefined(builder: InternalRowBuilder, index: Int): Boolean = {
    InternalRowBuilder.isValid(bytes, index)
  }

  def isDefined(builder: InternalRowBuilder, expr: Expression[_]): Boolean = {
    InternalRowBuilder.isValid(bytes, builder.exprIndex(expr))
  }

  def isNull(builder: InternalRowBuilder, expr: Expression[_]): Boolean = {
    !isDefined(builder, expr)
  }

  def isNull(builder: InternalRowBuilder, index: Int): Boolean = {
    !isDefined(builder, index)
  }

  def getRef(index: Int): AnyRef = {
    refs(index)
  }

  def getRef[T](builder: InternalRowBuilder, expr: Expression[T]): T = {
    val index = builder.exprIndex(expr)
    refs(index).asInstanceOf[T]
  }

  def get[T](builder: InternalRowBuilder, expr: Expression[T]): T = {
    val index = builder.exprIndex(expr)
    get[T](builder, index)(expr.dataType.internalStorable)
  }

  def get[T](builder: InternalRowBuilder, index: Int)(implicit storable: InternalStorable[T]): T = {
    if (builder.isFixed(index)) {
      getFixedLengthValue(builder, index)
    } else {
      getVariableLengthValue(builder, index)
    }
  }

  private def getFixedLengthValue[T](builder: InternalRowBuilder, index: Int)(
      implicit storable: InternalStorable[T]
  ): ID[T] = {
    val o = builder.fieldOffset(index)
    storable.read(bytes, o)
  }

  private def getVariableLengthValue[T](builder: InternalRowBuilder, index: Int)(
      implicit storable: InternalStorable[T]
  ): T = {
    val offset = builder.fieldOffset(index)
    val len = bytes.getInt(offset)
    if (len <= 12) {
      storable.read(bytes, offset + 4): ID[T]
    } else {
      val vOffset = bytes.getInt(offset + 4)
      builder.tmpBuffer.put(0, bytes, offset + 8, 8)
      builder.tmpBuffer.put(8, bytes, vOffset, len - 8)
      storable.read(builder.tmpBuffer, 0): ID[T]
    }
  }

//  def set[T](builder: InternalRowBuilder, index: Int, value: T)(implicit storable: InternalStorable[T]): Unit = {
//    builder.set(bytes, index, value)
//  }
//
//  def set[T](builder: InternalRowBuilder, expr: Expression[T], value: T): Unit = {
//    val index = builder.exprIndex(expr)
//    set(builder, index, value)(expr.dataType.internalStorable)
//  }
}

final class InternalRowBuilder(val exprIndex: Map[Expression[_], Int], table: Option[Table]) extends Serializable {

  import InternalRowBuilder._

  val SIZE_OF_FIXED_PART_OF_VARIABLE_LENGHT_FIELDS = 16

  val timeIndex: Int = exprIndex.getOrElse(TimeExpr, -1)

  val numOfFields: Int = exprIndex.values.max + 1
  val validityMapAreaSize: Int = (numOfFields / 64 + 1) * 8
  val fixedAreaOffset: Int = 4 + validityMapAreaSize
  val fixedAreaSize: Int = getFixedAreaSize(exprIndex)
  val variableLengthFieldsAreaOffset: Int = 4 + fixedAreaSize + validityMapAreaSize

  private val fieldsOffsets: Array[Int] = getFieldsOffsets(exprIndex)
  private val tagIndexMappingArray: Array[Int] = createTagByIndexMappingArray(exprIndex, table)
  private val fixedFieldsMappingArray: Array[Boolean] = createFixedFieldMappingArray(exprIndex)

  private var memoryBlock = MemoryBuffer.allocateHeap(MEMORY_BLOCK_SIZE)
  private var buffer = memoryBlock
  val tmpBuffer = MemoryBuffer.allocateHeap(MAX_ROW_SIZE)

  var refs: Map[Int, AnyRef] = Map.empty

  setRowSize(variableLengthFieldsAreaOffset)

  def this(queryContext: QueryContext) = this(queryContext.exprsIndex.toMap, queryContext.query.table)

  def getRowSize(): Int = {
    buffer.getInt(0)
  }

  private def setRowSize(size: Int) = {
    buffer.putInt(0, size)
  }

  def buildAndReset(): InternalRow = {
    val s = getRowSize()
    if (memoryBlock.size - memoryBlock.position() >= MAX_ROW_SIZE) {
      memoryBlock.position(memoryBlock.position() + s)
    } else {
      memoryBlock = MemoryBuffer.allocateHeap(MEMORY_BLOCK_SIZE)
    }
    val bf = buffer.asSlice(0, s)
    buffer = memoryBlock.asSlice(memoryBlock.position())
    setRowSize(variableLengthFieldsAreaOffset)
    clearValidityMapArea()
    val rf = refs
    refs = Map.empty[Int, AnyRef]
    new InternalRow(bf, rf)
  }

  //   | validity max |  fixed fields area |  variable length fields area |

  def set[T](tag: Byte, v: T)(implicit storable: InternalStorable[T]): InternalRowBuilder = {
    val index = tagIndex(tag)
    set(index, v)
  }

  def set[T](index: Int, v: T)(implicit storable: InternalStorable[T]): InternalRowBuilder = {
    set(buffer, index, v)
  }

  def set[T](buf: MemoryBuffer, index: Int, v: T)(implicit storable: InternalStorable[T]): InternalRowBuilder = {
    if (index != -1) {
      if (isFixed(index)) {
        setFixedLengthValue(buf, index, v)
      } else {
        setVariableLengthValue(buf, index, v)
      }
      InternalRowBuilder.setValid(buf, index)
    }
    this
  }

  def set[T](expr: Expression[T], v: T): InternalRowBuilder = {
    val idx = exprIndex(expr)
    set(idx, v)(expr.dataType.internalStorable)
  }

  def set(v: Time): InternalRowBuilder = {
    set(timeIndex, v)
    this
  }

  def setRef(index: Int, value: AnyRef): InternalRowBuilder = {
    refs += (index -> value)
    this
  }

  def setFieldsFromRow(row: InternalRow): InternalRowBuilder = {
    buffer.put(0, row.bytes, 0, row.bytes.size.toInt)
    this
  }

  def fieldsBuffer: MemoryBuffer = {
    buffer
  }
  def getExpr(index: Int): Expression[_] = {
    exprIndex.find(_._2 == index).get._1
  }

  def fieldOffset(index: Int): Int = {
    fieldsOffsets(index)
  }

  private def setFixedLengthValue[T](buf: MemoryBuffer, index: Int, v: T)(
      implicit storable: InternalStorable[T]
  ): Unit = {
    val offset = fieldsOffsets(index)
    if (offset >= fixedAreaOffset) {
      storable.write(buf, offset, v: ID[T])
    } else {
      throw new IllegalArgumentException("Wrong fixed filed offset offset")
    }
  }

  private def setVariableLengthValue[T](buf: MemoryBuffer, index: Int, v: T)(
      implicit storable: InternalStorable[T]
  ): Unit = {
    tmpBuffer.rewind()
    storable.write(tmpBuffer, v: ID[T])
    val len = tmpBuffer.position()
    tmpBuffer.rewind()
    setVariableLengthValueFromBuf(buf, index, tmpBuffer, len)
  }

  def setVariableLengthValueFromBuf(buf: MemoryBuffer, index: Int, srcBuf: MemoryBuffer, len: Int): Unit = {
    val offset = fieldsOffsets(index)
    if (offset >= fixedAreaOffset) {
      buf.putInt(offset, len)
      if (len <= 12) {
        buf.put(offset + 4, srcBuf, 0, len)
      } else {
        setBigVariableLengthValue(buf, index, srcBuf, len)
      }
    }
  }

  def setBigVariableLengthValue(buf: MemoryBuffer, index: Int, value: MemoryBuffer, length: Int): Unit = {
    val offset = fieldsOffsets(index)

    val vOffset = getRowSize()

    buf.putInt(offset + 4, vOffset)
    buf.put(offset + 8, value, 0, 8)
    buf.put(vOffset, value, 8, length - 8)
    setRowSize(vOffset + length - 8)
  }

  def isFixed(index: Int): Boolean = {
    fixedFieldsMappingArray(index)
  }
  def isValid(index: Int): Boolean = {
    InternalRowBuilder.isValid(buffer, index)
  }

  def setValid(index: Int): Unit = {
    InternalRowBuilder.setValid(buffer, index)
  }

  def setNull(index: Int): InternalRowBuilder = {
    InternalRowBuilder.setNull(buffer, index)
    this
  }

  def setNull(expr: Expression[_]): InternalRowBuilder = {
    setNull(exprIndex(expr))
  }

  def clearValidityMapArea() = {
    InternalRowBuilder.clearValidityMapArea(buffer, validityMapAreaSize)
  }

  def tagIndex(tag: Byte): Int = {
    tagIndexMappingArray(tag & 0xFF)
  }

  def setId(tag: Byte, v: String): Unit = {
    val index = dimIdIndex(tag)
    if (index != -1) {
      set(index, v)
    }
  }

  def needId(tag: Byte): Boolean = dimIdIndex(tag & 0xFF) != -1

  private def getFieldsOffsets(exprIndex: Map[Expression[_], Int]) = {
    val offsets = Array.fill[Int](numOfFields)(-1)
    exprIndex.toSeq
      .sortBy(_._2)
      .foldLeft(fixedAreaOffset) {
        case (offset, (expr, idx)) =>
          val size = expr.dataType.internalStorable.fixedSize.getOrElse(SIZE_OF_FIXED_PART_OF_VARIABLE_LENGHT_FIELDS)
          offsets(idx) = offset
          offset + size
      }
    offsets
  }

  private def getFixedAreaSize(exprIndex: Map[Expression[_], Int]) = {
    exprIndex.foldLeft(0) {
      case (s, (e, _)) =>
        s + e.dataType.internalStorable.fixedSize.getOrElse(SIZE_OF_FIXED_PART_OF_VARIABLE_LENGHT_FIELDS)
    }
  }

  private def createFixedFieldMappingArray(exprIndex: Map[Expression[_], Int]) = {

    val fixed = Array.fill[Boolean](numOfFields)(false)
    exprIndex.foreach {
      case (expr, index) =>
        fixed(index) = expr.dataType.internalStorable.fixedSize.isDefined
    }
    fixed
  }

  private def createTagByIndexMappingArray(exprIndex: Map[Expression[_], Int], table: Option[Table]): Array[Int] = {
    table match {
      case Some(t) =>
        val tagIndexes = Array.fill[Int](Table.MAX_TAGS)(-1)

        exprIndex.toSeq.foreach {
          case (expr, index) =>
            val tag = expr match {
              case MetricExpr(metric) =>
                Some(metric.tag)

              case DimensionExpr(dimension: Dimension) =>
                Some(t.dimensionTag(dimension))

              case _ =>
                None
            }
            tag.foreach { t =>
              tagIndexes(t & 0xFF) = index
            }
        }

        tagIndexes
      case None => Array.empty
    }
  }

  private val dimIdIndex: Array[Int] = table match {
    case Some(table) =>
      val tagIndex = Array.fill[Int](Table.MAX_TAGS)(-1)

      exprIndex.toSeq.foreach {
        case (DimensionIdExpr(dimension: Dimension), index) =>
          val t = table.dimensionTag(dimension)
          tagIndex(t & 0xFF) = index

        case _ =>
      }

      tagIndex

    case None =>
      Array.empty
  }

  private def readObject(ois: ObjectInputStream): Unit = {
    ois.defaultReadObject()
    setRowSize(variableLengthFieldsAreaOffset)
  }
}

object InternalRowBuilder {

  implicit val readerWriter: ReaderWriter[MemoryBuffer, ID, TypedInt] = MemoryBufferEvalReaderWriter

  val MAX_ROW_SIZE = 2_000_000
  val MEMORY_BLOCK_SIZE = MAX_ROW_SIZE * 2

  val validityMapAreaOffset = 4

  val zeoroValidityMapArea = MemoryBuffer.ofBytes(Array.ofDim[Byte]((MAX_ROW_SIZE / 64 + 1) * 8))

  def setValid(buf: MemoryBuffer, index: Int): Unit = {
    val o = validityMapAreaOffset + (index / 64) * 8
    val b = buf.getLong(o)
    val n = b | (1 << (index % 64))
    buf.putLong(o, n)
  }

  def setNull(buf: MemoryBuffer, index: Int): Unit = {
    val o = validityMapAreaOffset + (index / 64) * 8
    val b = buf.getLong(o)
    val mask = 1 << (index % 64)
    if ((b & mask) > 0) {
      val n = b ^ mask
      buf.putLong(o, n)
    }
  }

  def isValid(buf: MemoryBuffer, index: Int): Boolean = {
    val b = buf.getLong(validityMapAreaOffset + (index / 64) * 8)
    (b & (1 << (index % 64))) > 0
  }

  def clearValidityMapArea(buffer: MemoryBuffer, validityMapAreaSize: Int) = {
    validityMapAreaSize match {
      case 8 =>
        buffer.putLong(validityMapAreaOffset, 0)
      case 16 =>
        buffer.putLong(validityMapAreaOffset, 0)
        buffer.putLong(validityMapAreaOffset + 8, 0)
      case 24 =>
        buffer.putLong(validityMapAreaOffset, 0)
        buffer.putLong(validityMapAreaOffset + 8, 0)
        buffer.putLong(validityMapAreaOffset + 16, 0)
      case 32 =>
        buffer.putLong(validityMapAreaOffset, 0)
        buffer.putLong(validityMapAreaOffset + 8, 0)
        buffer.putLong(validityMapAreaOffset + 16, 0)
        buffer.putLong(validityMapAreaOffset + 24, 0)
      case _ =>
        buffer.put(validityMapAreaOffset, zeoroValidityMapArea, 0, validityMapAreaSize)
    }
  }

  def getBytesSize(buffer: MemoryBuffer): Int = {
    buffer.getInt(0)
  }
}
