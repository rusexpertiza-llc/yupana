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

import org.yupana.api.query.Expression
import org.yupana.api.types.InternalStorable
import org.yupana.core.QueryContext
import org.yupana.serialization.MemoryBuffer

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

final class HashTableDataset(schema: DatasetSchema) {
  import HashTableDataset._

  private val hashMap = new mutable.AnyRefMap[AnyRef, RowPointer]

  private var batches = Array(new BatchDataset(schema: DatasetSchema))

  def iterator: Iterator[BatchDataset] = batches.iterator

  def contains(key: AnyRef): Boolean = {
    hashMap.contains(key)
  }

  def partition(numOfPartitions: Int): Seq[(Int, Array[BatchDataset])] = {
    val partitions = Array.fill(numOfPartitions) { ArrayBuffer.empty[BatchDataset] }
    hashMap.foreachEntry { (key, ptr) =>
      val partition = math.abs(key.hashCode() % numOfPartitions)
      val partBatches = partitions(partition)
      val dstBatch = if (partBatches.nonEmpty && partBatches.last.size < partBatches.last.capacity) {
        partBatches.last
      } else {
        val newBatch = new BatchDataset(schema)
        partBatches.append(newBatch)
        newBatch
      }
      val srcBatch = batch(ptr)
      val srcRowNum = rowNumber(ptr)
      dstBatch.copyRowFrom(dstBatch.size, srcBatch, srcRowNum)
    }
    partitions.map(_.toArray).toSeq.zipWithIndex.map(_.swap)
  }

  def isDefined(key: AnyRef, expr: Expression[_]): Boolean = {
    val ptr = hashMap.getOrElse(key, KEY_NOT_FOUND)
    if (ptr != KEY_NOT_FOUND) {
      val rowNum = rowNumber(ptr)
      batch(ptr).isDefined(rowNum, expr)
    } else {
      false
    }
  }

  def isDefined(key: AnyRef, fieldIndex: Int): Boolean = {
    val ptr = hashMap.getOrElse(key, KEY_NOT_FOUND)
    if (ptr != KEY_NOT_FOUND) {
      val rowNum = rowNumber(ptr)
      batch(ptr).isDefined(rowNum, fieldIndex)
    } else {
      false
    }
  }

  def isNull(key: AnyRef, expr: Expression[_]): Boolean = {
    !isDefined(key, expr)
  }

  def isNull(key: AnyRef, fieldIndex: Int): Boolean = {
    !isDefined(key, fieldIndex)
  }

  def isNullRef(key: AnyRef, expression: Expression[_]): Boolean = {
    val ptr = hashMap.getOrElse(key, KEY_NOT_FOUND)
    if (ptr != KEY_NOT_FOUND) {
      val rowNum = rowNumber(ptr)
      val ordinal = schema.refFieldOrdinal(expression)
      batch(ptr).isNullRef(rowNum, ordinal)
    } else {
      false
    }
  }

  def setNull(key: AnyRef, fieldIndex: Int): Unit = {
    val ptr = hashMap.getOrElse(key, KEY_NOT_FOUND)
    if (ptr != KEY_NOT_FOUND) {
      val rowNum = rowNumber(ptr)
      batch(ptr).setNull(rowNum, fieldIndex)
    }
  }

  def get[T](key: AnyRef, expr: Expression[T]): T = {
    get(key, schema.fieldIndex(expr))(expr.dataType.internalStorable)
  }

  def get[T](key: AnyRef, fieldIndex: Int)(implicit storable: InternalStorable[T]): T = {
    val ptr = hashMap.getOrElse(key, KEY_NOT_FOUND)
    val rowNum = rowNumber(ptr)
    batch(ptr).get(rowNum, fieldIndex)
  }

  def fieldBufferForRead(key: AnyRef, fieldIndex: Int): MemoryBuffer = {
    val ptr = hashMap.getOrElse(key, KEY_NOT_FOUND)
    batch(ptr).fieldBufferForRead(rowNumber(ptr), fieldIndex)
  }

  def fieldBufferForReadOffset(key: AnyRef, fieldIndex: Int): Int = {
    val ptr = hashMap.getOrElse(key, KEY_NOT_FOUND)
    batch(ptr).fieldBufferForReadOffset(rowNumber(ptr), fieldIndex)
  }

  def set[T](key: AnyRef, expr: Expression[T], v: T): Unit = {
    set(key, schema.fieldIndex(expr), v)(expr.dataType.internalStorable)
  }

  def set[T](key: AnyRef, fieldIndex: Int, value: T)(implicit storable: InternalStorable[T]): Unit = {
    val ptr = putKeyToHashMap(key)
    batch(ptr).set(rowNumber(ptr), fieldIndex, value)
  }

  def fieldBufferForWrite(key: AnyRef, fieldIndex: Int): MemoryBuffer = {
    val ptr = putKeyToHashMap(key)
    batch(ptr).fieldBufferForWrite(rowNumber(ptr), fieldIndex)
  }

  def fieldBufferForWriteOffset(key: AnyRef, fieldIndex: Int): Int = {
    val ptr = putKeyToHashMap(key)
    batch(ptr).fieldBufferForWriteOffset(rowNumber(ptr), fieldIndex)
  }

  def writeFieldBuffer(key: AnyRef, fieldIndex: Int, size: Int): Unit = {
    val ptr = hashMap.getOrElse(key, KEY_NOT_FOUND)
    batch(ptr).writeFieldBuffer(rowNumber(ptr), fieldIndex, size)
  }

  def getRef[T](key: AnyRef, expr: Expression[T]): T = {
    getRef(key, schema.refFieldOrdinal(expr))
  }

  def getRef[T](key: AnyRef, ordinal: Int): T = {
    val ptr = rowPointer(key)
    val rowNum = rowNumber(ptr)
    batch(ptr).getRef(rowNum, ordinal)
  }

  def setRef(key: AnyRef, expr: Expression[_], value: AnyRef): Unit = {
    setRef(key, schema.refFieldOrdinal(expr), value)
  }

  def setRef(key: AnyRef, ordinal: Int, value: AnyRef): Unit = {
    val ptr = putKeyToHashMap(key)
    batch(ptr).setRef(rowNumber(ptr), ordinal, value)
  }

  private def putKeyToHashMap(key: AnyRef): RowPointer = {
    val ptr = hashMap.getOrElse(key, KEY_NOT_FOUND)
    if (ptr != KEY_NOT_FOUND) {
      ptr
    } else {
      val newPtr = newRow()
      hashMap.update(key, newPtr)
      newPtr
    }
  }

  def updateKey(key: AnyRef, rowPtr: RowPointer): Unit = {
    hashMap.update(key, rowPtr)
  }

  def newRow(): RowPointer = {
    val batch: BatchDataset = batchForNewRow()
    val rowNum = batch.size
    createPtr(batches.length - 1, rowNum)
  }

  def createPtr(bathIndex: Int, rowNum: Int): RowPointer = {
    (bathIndex.toLong << 32) + rowNum.toLong
  }

  def rowPointer(key: AnyRef): RowPointer = {
    hashMap.getOrElse(key, KEY_NOT_FOUND)
  }

  def rowNumber(ptr: RowPointer): Int = {
    (ptr & 0xFFFFFFFFL).toInt
  }

  def batch(ptr: RowPointer): BatchDataset = {
    val batchIdx = (ptr >> 32).toInt
    batches(batchIdx)
  }

  def batchForNewRow(): BatchDataset = {
    val lastBatch = batches(batches.length - 1)
    val batch = if (lastBatch.size < lastBatch.capacity) {
      lastBatch
    } else {
      addBatch()
    }
    batch
  }

  private def addBatch[T](): BatchDataset = {
    batches = Array.copyOf(batches, batches.length + 1)
    val newBatch = new BatchDataset(schema)
    batches(batches.length - 1) = newBatch
    newBatch
  }
}

object HashTableDataset {
  private type RowPointer = Long

  val KEY_NOT_FOUND: RowPointer = -1L

  def apply(qc: QueryContext): HashTableDataset = {
    new HashTableDataset(qc.datasetSchema)
  }
}
