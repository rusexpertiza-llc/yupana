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

package org.yupana.khipu.storage

import jdk.internal.vm.annotation.ForceInline
import org.yupana.api.schema.Table
import org.yupana.api.utils.DimOrdering

import java.lang.foreign.{ Arena, MemorySegment, ValueLayout }
import java.nio.ByteOrder
import java.util.Comparator

object StorageFormat {

  final val INT_LAYOUT = ValueLayout.JAVA_INT.withOrder(ByteOrder.BIG_ENDIAN)
  final val INT_LAYOUT_UNALIGNED = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN)
  final val LONG_LAYOUT = ValueLayout.JAVA_LONG.withOrder(ByteOrder.BIG_ENDIAN)
  final val LONG_LAYOUT_UNALIGNED = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN)
  final val SHORT_LAYOUT = ValueLayout.JAVA_SHORT.withOrder(ByteOrder.BIG_ENDIAN)
  final val BYTE_LAYOUT = ValueLayout.JAVA_BYTE.withOrder(ByteOrder.BIG_ENDIAN)

  implicit val byteArrayDimOrdering: DimOrdering[Array[Byte]] =
    DimOrdering.fromCmp(StorageFormat.BYTES_COMPARATOR.compare)

//  import java.nio.ByteOrder

  @ForceInline
  final def getInt(segment: MemorySegment, offset: Long): Int = {
    segment.get(INT_LAYOUT, offset)
  }

  @ForceInline
  final def getIntUnaligned(segment: MemorySegment, offset: Long): Int = {
    segment.get(INT_LAYOUT_UNALIGNED, offset)
  }

  @ForceInline
  final def setInt(v: Int, segment: MemorySegment, offset: Long): Unit = {
    segment.set(INT_LAYOUT, offset, v)
  }

  @ForceInline
  final def setIntUnaligned(v: Int, segment: MemorySegment, offset: Long): Unit = {
    segment.set(INT_LAYOUT_UNALIGNED, offset, v)
  }

  @ForceInline
  final def getLong(segment: MemorySegment, offset: Long): Long = {
    segment.get(LONG_LAYOUT, offset)
  }

  @ForceInline
  final def getLongUnaligned(segment: MemorySegment, offset: Long): Long = {
    segment.get(LONG_LAYOUT_UNALIGNED, offset)
  }

  @ForceInline
  final def setLong(v: Long, segment: MemorySegment, offset: Long): Unit = {
    segment.set(LONG_LAYOUT, offset, v)
  }

  @ForceInline
  final def setLongUnaligned(v: Long, segment: MemorySegment, offset: Long): Unit = {
    segment.set(LONG_LAYOUT_UNALIGNED, offset, v)
  }

  @ForceInline
  final def getByte(segment: MemorySegment, offset: Long): Byte = {
    segment.get(BYTE_LAYOUT, offset)
  }

  @ForceInline
  final def setByte(v: Byte, segment: MemorySegment, offset: Long): Unit = {
    segment.set(BYTE_LAYOUT, offset, v)
  }

  @ForceInline
  final def getShort(segment: MemorySegment, offset: Long): Short = {
    segment.get(SHORT_LAYOUT, offset)
  }

  @ForceInline
  final def setShort(v: Short, segment: MemorySegment, offset: Long): Unit = {
    segment.set(SHORT_LAYOUT, offset, v)
  }

  @ForceInline
  final def getBytes(src: MemorySegment, srcOffset: Long, dst: Array[Byte], size: Int): Unit = {
    MemorySegment.copy(src, BYTE_LAYOUT, srcOffset, dst, 0, size)
  }

  @ForceInline
  def getBytes(src: MemorySegment, srcOffset: Long, size: Int): Array[Byte] = {
    val a = Array.ofDim[Byte](size)
    getBytes(src, srcOffset, a, size)
    a
  }

  @ForceInline
  final def setBytes(src: Array[Byte], srcOffset: Int, dst: MemorySegment, dstOffset: Long, size: Int): Unit = {
    MemorySegment.copy(src, srcOffset, dst, BYTE_LAYOUT, dstOffset, size)
  }

  @ForceInline
  final def getSegment(src: MemorySegment, srcOffset: Long, size: Int): MemorySegment = {
    src.asSlice(srcOffset, size)
  }

  @ForceInline
  final def fromBytes(a: Array[Byte]): MemorySegment = {
    val keySegment = allocateHeap(a.length)
    MemorySegment.copy(a, 0, keySegment, BYTE_LAYOUT, 0, a.length)
    keySegment
  }
  @ForceInline
  final def copy(src: MemorySegment, dst: MemorySegment, dstOffset: Long): Unit = {
    MemorySegment.copy(src, 0, dst, dstOffset, src.byteSize())
  }

  def keySize(table: Table): Int = {
    java.lang.Long.BYTES + // base time
      table.dimensionSeq.map(_.rStorable.size).sum + // dimensions
      java.lang.Long.BYTES // rest time
  }
  @inline
  final def startsWith(buf: MemorySegment, prefix: MemorySegment, limit: Int): Boolean = {

    val bufLen = Math.min(buf.byteSize(), limit)

    val prefixLen = prefix.byteSize()
    if (bufLen >= prefixLen) {
      val minLen = Math.min(bufLen, prefixLen)
      val step = 8
      val stepsLimit = (minLen / 8) * 8

      var commonPrefix = 0

      while (
        commonPrefix < stepsLimit &&
        getLong(buf, commonPrefix) == getLong(prefix, commonPrefix)
      ) {
        commonPrefix += step
      }

      if (commonPrefix == prefixLen) {
        true
      } else if (commonPrefix == stepsLimit) {
        while (commonPrefix < prefixLen && getByte(buf, commonPrefix) == getByte(prefix, commonPrefix)) {
          commonPrefix += 1
        }
        commonPrefix == prefixLen
      } else {
        false
      }
    } else {
      false
    }
  }

  final val BYTES_COMPARATOR: Comparator[Array[Byte]] = new Comparator[Array[Byte]] {
    override def compare(o1: Array[Byte], o2: Array[Byte]): Int = {
      StorageFormat.compareTo(o1, 0, o2, 0, math.max(o1.length, o2.length))
    }
  }

  final val ROWS_COMPARATOR: Comparator[Row] = new Comparator[Row] {
    override def compare(o1: Row, o2: Row): Int = {
      StorageFormat.compareTo(o1.key, o2.key, math.max(o1.key.byteSize(), o2.key.byteSize()).toInt)
    }
  }

  final val SEGMENT_COMPARATOR: Comparator[MemorySegment] = new Comparator[MemorySegment] {
    override def compare(o1: MemorySegment, o2: MemorySegment): Int = {
      StorageFormat.compareTo(o1, o2, math.max(o1.byteSize(), o2.byteSize()).toInt)
    }
  }

  def compareTo(a1: Array[Byte], a2: Array[Byte], size: Int): Int = {
    compareTo(a1, 0, a2, 0, size)
  }

  def compareTo2(a1: Array[Byte], offset1: Int, a2: Array[Byte], offset2: Int, size: Int): Int = {
    if (a1 eq a2) {
      0
    } else {

      val n = math.min(a1.length, size)
      val m = math.min(a2.length, size)
      var i = 0
      var j = 0
      while (i < n && j < m) {
        val x = a1(i) & 0xFF
        val y = a2(j) & 0xFF
        if (x != y) return x - y

        i += 1
        j += 1
      }
      n - m
    }
  }

  def compareTo(a1: Array[Byte], offset1: Int, a2: Array[Byte], offset2: Int, size: Int): Int = {
    if ((a1 eq a2) && offset1 == offset2) return 0
    compareTo(MemorySegment.ofArray(a1), offset1, MemorySegment.ofArray(a2), offset2, size)
  }

  def compareTo(a1: MemorySegment, a2: MemorySegment, size: Int): Int = {
    compareTo(a1, 0, a2, 0, size)
  }
  def compareTo(a1: MemorySegment, offset1: Int, a2: MemorySegment, offset2: Int, size: Int): Int = {
    // Short circuit equal case
    if ((a1 eq a2) && offset1 == offset2) return 0

    val sizeA1 = math.min(size, a1.byteSize() - offset1)
    val sizeA2 = math.min(size, a2.byteSize() - offset2)

    val minSize = math.min(sizeA1, sizeA2)
    val stride = 8
    val strideLimit = minSize & ~(stride - 1)

    var i = 0
    /*
     * Compare 8 bytes at a time. Benchmarking on x86 shows a stride of 8 bytes is no slower
     * than 4 bytes even on 32-bit. On the other hand, it is substantially faster on 64-bit.
     */
    i = 0
    while (i < strideLimit) {
      val lw = getLong(a1, offset1 + i)
      val rw = getLong(a2, offset2 + i)
      if (lw != rw) {
        return java.lang.Long.compareUnsigned(lw, rw)
      }
      i += stride
    }
    // The epilogue to cover the last (minLength % stride) elements.

    while (i < minSize) {
      val a = getByte(a1, offset1 + i) & 0xFF
      val b = getByte(a2, offset2 + i) & 0xFF
      if (a != b) return a - b
      i += 1
    }

    if (sizeA1 > sizeA2) 1 else if (sizeA1 == sizeA2) 0 else -1
  }

  def incMemorySegment(segment: MemorySegment, size: Int): MemorySegment = {
    val bytes = segment.toArray(BYTE_LAYOUT)
    val t = (BigInt(1, bytes) + 1).toByteArray
    val s = allocateHeap(size)
    MemorySegment.copy(t, 0, s, BYTE_LAYOUT, size - t.length, t.length)
    s
  }

  def alignInt(size: Int): Int = {
    align(size, 4)
  }

  @ForceInline
  def alignLong(size: Int): Int = {
    align(size, 8)

  }

  @ForceInline
  def align(value: Int, align: Int) = {
    (value + align - 1) & ~(align - 1)
  }

  def allocateNative(size: Int): MemorySegment = {
    Arena.ofConfined().allocate(size, 8)
  }

  def allocateHeap(size: Int): MemorySegment = {
    val array = Array.ofDim[Long]((size + 7) / 8)
    MemorySegment.ofArray(array).asSlice(0, size)
  }
}
