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

import jdk.internal.vm.annotation.ForceInline
import org.yupana.api.Time
import org.yupana.api.query.Expression
import org.yupana.api.types.{ ID, InternalReaderWriter, InternalStorable }
import org.yupana.core.QueryContext
import org.yupana.serialization.{ MemoryBuffer, MemoryBufferEvalReaderWriter }

final class BatchDataset(val schema: DatasetSchema, val capacity: Int = BatchDataset.MAX_MUM_OF_ROWS)
    extends Serializable {
  import BatchDataset._

  private val bitSet: Array[Long] = Array.ofDim(capacity * (schema.numOfFields + 1) / 64 + 1)

  private val fixedLengthFieldsArea: MemoryBuffer =
    MemoryBuffer.allocateHeap(capacity * schema.fixedLengthFieldsBytesSize)

  private val variableLengthFieldsArea: MemoryBuffer =
    MemoryBuffer.allocateHeap(capacity * INIT_VAR_LEN_FIELDS_AREA_SIZE_CAPAITY_FACTOR)
  private var variableLengthFieldsAreaSize: Int = 0

  private val refs: Array[AnyRef] = Array.ofDim(capacity * schema.numOfRefFields)

  private val tmpBuffer = MemoryBuffer.allocateHeap(TMP_BUF_SIZE)

  private var maxRowNumber = -1

  def size: Int = maxRowNumber + 1

  def foreach(f: Int => Unit): Unit = {
    var rowNum = 0
    while (rowNum < size) {
      if (!isDeleted(rowNum)) {
        f(rowNum)
      }
      rowNum += 1
    }
  }

  def copyRowFrom(rowNum: Int, src: BatchDataset, srcRowNum: Int): Unit = {
    fixedLengthFieldsArea.put(
      getOffset(rowNum),
      src.fixedLenFieldsBuffer(),
      src.getOffset(srcRowNum),
      schema.fixedLengthFieldsBytesSize
    )


    var fieldIndex = 0
    while (fieldIndex < schema.numOfFields) {
      if (schema.isRef(fieldIndex)) {
        val ordinal = schema.refFieldOrdinal(fieldIndex)
        val ref = src.getRef[AnyRef](srcRowNum, ordinal)
        setRef(rowNum, ordinal, ref)
      }

      val offset = getOffset(rowNum, fieldIndex)
      if (!schema.isFixed(fieldIndex) && !schema.isRef(fieldIndex)) {
        val len = fixedLengthFieldsArea.getInt(offset)
        if (len > 12) {
          val srcVOffset = fixedLengthFieldsArea.getInt(offset + 4)
          val vOffset = variableLengthFieldsAreaSize
          resizeVarLengthFieldsAreaSize(variableLengthFieldsAreaSize + len - 8)
          variableLengthFieldsArea.put(
            vOffset,
            src.variableLengthFieldsArea,
            srcVOffset,
            len - 8
          )
        }
      }
      fieldIndex += 1
    }
    updateSize(rowNum)
  }

  def setDeleted(rowNum: Int): Unit = {
    BitSetOps.set(bitSet, rowDeletionBitNum(rowNum))
  }

  def removeDeleted(rowNum: Int): Unit = {
    BitSetOps.clear(bitSet, rowDeletionBitNum(rowNum))
    updateSize(rowNum)
  }

  def isDeleted(rowNum: Int): Boolean = {
    BitSetOps.check(bitSet, rowDeletionBitNum(rowNum))
  }

  def isNull(rowNum: Int, expr: Expression[_]): Boolean = {
    !isDefined(rowNum, expr)
  }

  def isNull(rowNum: Int, index: Int): Boolean = {
    !isDefined(rowNum, index)
  }

  def isDefined(rowNum: Int, expr: Expression[_]): Boolean = {
    val fieldIndex = schema.fieldIndex(expr)
    if (schema.isValue(fieldIndex)) {
      isDefined(rowNum, fieldIndex)
    } else {
      !isNullRef(rowNum, schema.refFieldOrdinal(fieldIndex))
    }
  }

  def isDefined(rowNum: Int, fieldIdx: Int): Boolean = {
    BitSetOps.check(bitSet, fieldValidityBitNum(rowNum, fieldIdx))
  }

  @ForceInline
  def setValid(rowNum: Int, fieldIdx: Int): Unit = {
    BitSetOps.set(bitSet, fieldValidityBitNum(rowNum, fieldIdx))
  }

  def setNull(rowNum: Int, expr: Expression[_]): Unit = {
    setNull(rowNum, schema.fieldIndex(expr))
  }

  def setNull(rowNum: Int, fieldIdx: Int): Unit = {
    BitSetOps.clear(bitSet, fieldValidityBitNum(rowNum, fieldIdx))
  }

  def isRef(rowNum: Int, fieldIndex: Int): Boolean = {
    schema.isRef(fieldIndex)
  }

  def getRef[T](rowNum: Int, expr: Expression[T]): T = {
    val fieldIdx = schema.refFieldOrdinal(expr)
    getRef(rowNum, fieldIdx)
  }

  def getRef[T](rowNum: Int, ordinal: Int): T = {
    val pointer = getRefPointer(rowNum, ordinal)
    refs(pointer).asInstanceOf[T]
  }

  def getRefPointer(rowNum: Int, ordinal: Int): Int = {
    rowNum * schema.numOfRefFields + ordinal
  }

  def setRef(rowNum: Int, expression: Expression[_], ref: AnyRef): Unit = {
    val ordinal = schema.refFieldOrdinal(expression)
    setRef(rowNum, ordinal, ref)
  }

  def setRef(rowNum: Int, ordinal: Int, ref: AnyRef): Unit = {
    val pointer = getRefPointer(rowNum, ordinal)
    refs(pointer) = ref
  }

  def isNullRef(rowNum: Int, expr: Expression[_]): Boolean = {
    isNullRef(rowNum, schema.refFieldOrdinal(expr))
  }

  def isNullRef(rowNum: Int, ordinal: Int): Boolean = {
    val pointer = getRefPointer(rowNum, ordinal)
    refs(pointer) == null
  }

  def set[T](rowNumber: Int, tag: Byte, v: T)(implicit storable: InternalStorable[T]): Unit = {
    val fieldIndex = schema.fieldIndex(tag)
    set(rowNumber, fieldIndex, v)
  }

  def set[T](rowNumber: Int, expr: Expression[T], v: T): Unit = {
    val idx = schema.fieldIndex(expr)
    set(rowNumber, idx, v)(expr.dataType.internalStorable)
  }

  def set(rowNumber: Int, v: Time): Unit = {
    set(rowNumber, schema.timeFieldIndex, v)
  }

  def set[T](rowNumber: Int, fieldIndex: Int, v: T)(implicit storable: InternalStorable[T]): Unit = {
    if (fieldIndex != -1) {

      if (schema.isValue(fieldIndex)) {
        val buf = fieldBufferForWrite(rowNumber, fieldIndex)
        val offset = fieldBufferForWriteOffset(rowNumber, fieldIndex)
        val size = storable.write(buf, offset, v: ID[T])
        writeFieldBuffer(rowNumber, fieldIndex, size)
        setValid(rowNumber, fieldIndex)
      } else {
        setRef(rowNumber, schema.refFieldOrdinal(fieldIndex), v.asInstanceOf[AnyRef])
      }
      updateSize(rowNumber)
    }
  }

  def get[T](rowNumber: Int, expr: Expression[T]): T = {
    val index = schema.fieldIndex(expr)
    get[T](rowNumber, index)(expr.dataType.internalStorable)
  }

  def get[T](rowNumber: Int, fieldIndex: Int)(implicit storable: InternalStorable[T]): T = {
    if (schema.isValue(fieldIndex)) {
      val buf = fieldBufferForRead(rowNumber, fieldIndex)
      val offset = fieldBufferForReadOffset(rowNumber, fieldIndex)
      val size = fieldSize(rowNumber, fieldIndex)
      val a = storable.read(buf, offset, size)
      a
    } else {
      getRef[T](rowNumber, schema.refFieldOrdinal(fieldIndex))
    }
  }

  def getTime(rowNumber: Int): Time = {
    get[Time](rowNumber, schema.timeFieldIndex)
  }

  def setId(rowNumber: Int, tag: Byte, v: String): Unit = {
    val index = schema.dimIdFieldIndex(tag)
    if (index != -1) {
      set(rowNumber, index, v)
    }
  }

  def fixedLenFieldsBuffer(): MemoryBuffer = {
    fixedLengthFieldsArea
  }

  def fieldBufferForRead(rowNum: Int, fieldIndex: Int): MemoryBuffer = {
    if (schema.isFixed(fieldIndex)) {
      fixedLengthFieldsArea
    } else {
      fieldBufferForReadVarLenField(rowNum, fieldIndex)
    }
  }

  def fieldBufferForReadVarLenField(rowNum: Int, fieldIndex: Int): MemoryBuffer = {
    val offset = getOffset(rowNum, fieldIndex)
    fieldBufferForReadVarLenField(offset)
  }

  def fieldBufferForReadVarLenField(offset: Int): MemoryBuffer = {
    val len = fixedLengthFieldsArea.getInt(offset)
    if (len <= 12) {
      fixedLengthFieldsArea
    } else {
      val vOffset = fixedLengthFieldsArea.getInt(offset + 4)
      //        tmpBuffer.put(0, fixedLengthFieldsArea, offset + 8, 8)
      val l = fixedLengthFieldsArea.getLong(offset + 8)
      tmpBuffer.putLong(0, l)
      tmpBuffer.put(8, variableLengthFieldsArea, vOffset, len - 8)
      tmpBuffer
    }
  }

  def fieldBufferForReadOffset(rowNum: Int, fieldIndex: Int): Int = {
    if (schema.isFixed(fieldIndex)) {
      getOffset(rowNum, fieldIndex)
    } else {
      fieldBufferForReadVarLenFieldOffset(rowNum: Int, fieldIndex: Int)
    }
  }

  def fieldBufferForReadVarLenFieldOffset(rowNum: Int, fieldIndex: Int): Int = {
    val offset = getOffset(rowNum, fieldIndex)
    fieldBufferForReadVarLenFieldOffset(offset)
  }

  def fieldBufferForReadVarLenFieldOffset(offset: Int): Int = {
    val len = fixedLengthFieldsArea.getInt(offset)
    if (len <= 12) {
      offset + 4
    } else {
      0
    }
  }

  def fieldSize(rowNum: Int, fieldIndex: Int): Int = {
    if (schema.isFixed(fieldIndex)) {
      schema.fieldSize(fieldIndex)
    } else {
      varLenFieldSize(rowNum, fieldIndex)
    }
  }

  def varLenFieldSize(rowNum: Int, fieldIndex: Int): Int = {
    val offset = getOffset(rowNum, fieldIndex)
    varLenFieldSize(offset)
  }

  def varLenFieldSize(offset: Int): Int = {
    fixedLengthFieldsArea.getInt(offset)
  }

  def fieldBufferForWrite(rowNum: Int, fieldIndex: Int): MemoryBuffer = {
    if (schema.isFixed(fieldIndex)) {
      fixedLengthFieldsArea
    } else {
      tmpBuffer.rewind()
      tmpBuffer
    }
  }

  def fieldBufferForWriteOffset(rowNum: Int, fieldIndex: Int): Int = {
    val offset = getOffset(rowNum, fieldIndex)
    if (schema.isFixed(fieldIndex)) {
      offset
    } else {
      0
    }
  }

  def writeFieldBuffer(rowNum: Int, fieldIndex: Int, size: Int): Unit = {
    if (!schema.isFixed(fieldIndex)) {
      setVariableLengthValueFromBuf(rowNum, fieldIndex, tmpBuffer, size)
    }
  }

  private def setVariableLengthValueFromBuf(rowNumber: Int, fieldIndex: Int, srcBuf: MemoryBuffer, len: Int): Unit = {
    val offset = getOffset(rowNumber, fieldIndex)
    if (len <= 12) {
      fixedLengthFieldsArea.put(offset + 4, srcBuf, 0, len)
    } else {
      setBigVariableLengthValue(rowNumber, fieldIndex, srcBuf, len)
    }
    fixedLengthFieldsArea.putInt(offset, len)
  }

  private def setBigVariableLengthValue(rowNumber: Int, fieldIndex: Int, value: MemoryBuffer, length: Int): Unit = {
    val offset = getOffset(rowNumber, fieldIndex)

    fixedLengthFieldsArea.put(offset + 8, value, 0, 8)
    val vOffset = if (isDefined(rowNumber, fieldIndex) && length <= fixedLengthFieldsArea.getInt(offset)) {
      fixedLengthFieldsArea.getInt(offset + 4)
    } else {
      val newVOffset = variableLengthFieldsAreaSize
      resizeVarLengthFieldsAreaSize(variableLengthFieldsAreaSize + length - 8)
      newVOffset
    }
    fixedLengthFieldsArea.putInt(offset + 4, vOffset)
    fixedLengthFieldsArea.put(offset + 8, value, 0, 8)
    variableLengthFieldsArea.put(vOffset, value, 8, length - 8)
  }

  private def resizeVarLengthFieldsAreaSize(newSize: Int): Unit = {
    if (variableLengthFieldsArea.size < newSize) {
      variableLengthFieldsArea.reallocateHeap(newSize * 2)
    }
    variableLengthFieldsAreaSize = newSize
  }

  @ForceInline
  private def getOffset(rowNumber: Int, fieldIndex: Int): Int = {
    rowNumber * schema.fixedLengthFieldsBytesSize + schema.fieldOffset(fieldIndex)
  }

  private def getOffset(rowNumber: Int): Int = {
    rowNumber * schema.fixedLengthFieldsBytesSize
  }

  @ForceInline
  def updateSize(rowNum: Int): Unit = {
    if (maxRowNumber < rowNum) maxRowNumber = rowNum
  }

  private def rowDeletionBitNum(rowNum: Int) = {
    rowNum * (schema.numOfFields + 1)
  }
  private def fieldValidityBitNum(rowNum: Int, fieldIdx: Int) = {
    rowNum * (schema.numOfFields + 1) + fieldIdx + 1
  }
}

object BatchDataset {
  def apply(qc: QueryContext) = {
    new BatchDataset(qc.internalRowSchema)
  }

  implicit val readerWriter: InternalReaderWriter[MemoryBuffer, ID, Int, Int] = MemoryBufferEvalReaderWriter

  val MAX_MUM_OF_ROWS = 100000
  val TMP_BUF_SIZE = 2_000_000
  val SIZE_OF_FIXED_PART_OF_VARIABLE_LENGHT_FIELDS = 16
  val INIT_VAR_LEN_FIELDS_AREA_SIZE_CAPAITY_FACTOR = 2
}
