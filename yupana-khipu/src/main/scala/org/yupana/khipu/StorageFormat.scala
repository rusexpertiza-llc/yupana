package org.yupana.khipu

import jdk.incubator.foreign.{ MemoryAccess, MemorySegment }
import jdk.internal.foreign.AbstractMemorySegmentImpl
import jdk.internal.misc.Unsafe
import org.yupana.api.Time
import org.yupana.api.schema.{ Dimension, RawDimension, Table }
import org.yupana.core.model.InternalRowBuilder

import java.nio.ByteBuffer
import java.util.Comparator

object StorageFormat {

  private val UNSAFE = Unsafe.getUnsafe
  private val BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(classOf[Array[Byte]])

//  import java.nio.ByteOrder

  @inline
  def getInt(segment: MemorySegment, offset: Long): Int = {
    val s = segment.asInstanceOf[AbstractMemorySegmentImpl]
    UNSAFE.getInt(s.unsafeGetBase(), s.unsafeGetOffset() + offset)
  }

  @inline
  final def setInt(v: Int, segment: MemorySegment, offset: Long): Unit = {
    val s = segment.asInstanceOf[AbstractMemorySegmentImpl]
    UNSAFE.putInt(s.unsafeGetBase(), s.unsafeGetOffset() + offset, v)
  }

  @inline
  final def getLong(segment: MemorySegment, offset: Long): Long = {
    val s = segment.asInstanceOf[AbstractMemorySegmentImpl]
    UNSAFE.getLong(s.unsafeGetBase(), s.unsafeGetOffset() + offset)
  }

  @inline
  final def getByte(segment: MemorySegment, offset: Long): Byte = {
    val s = segment.asInstanceOf[AbstractMemorySegmentImpl]
    UNSAFE.getByte(s.unsafeGetBase(), s.unsafeGetOffset() + offset)
  }

  @inline
  final def setByte(v: Byte, segment: MemorySegment, offset: Long): Unit = {
    val s = segment.asInstanceOf[AbstractMemorySegmentImpl]
    UNSAFE.putByte(s.unsafeGetBase(), s.unsafeGetOffset() + offset, v)
  }

  @inline
  final def getShort(segment: MemorySegment, offset: Long): Short = {
    val s = segment.asInstanceOf[AbstractMemorySegmentImpl]
    UNSAFE.getShort(s.unsafeGetBase(), s.unsafeGetOffset() + offset)
  }

  @inline
  final def setShort(v: Short, segment: MemorySegment, offset: Long): Unit = {
    val s = segment.asInstanceOf[AbstractMemorySegmentImpl]
    UNSAFE.putShort(s.unsafeGetBase(), s.unsafeGetOffset() + offset, v)
  }

//  @inline
//  def getInt(segment: MemorySegment, offset: Long): Int = {
//    MemoryAccess.getIntAtOffset(segment, offset, ByteOrder.BIG_ENDIAN)
//  }
//
//  @inline
//  final def setInt(v: Int, segment: MemorySegment, offset: Long): Unit = {
//    MemoryAccess.setIntAtOffset(segment, offset, ByteOrder.BIG_ENDIAN, v)
//  }
//
//  @inline
//  final def getLong(segment: MemorySegment, offset: Long): Long = {
//    MemoryAccess.getLongAtOffset(segment, offset, ByteOrder.BIG_ENDIAN)
//  }
//
//  @inline
//  final def getByte(segment: MemorySegment, offset: Long): Byte = {
//    MemoryAccess.getByteAtOffset(segment, offset)
//  }
//
//  @inline
//  final def setByte(v: Byte, segment: MemorySegment, offset: Long): Unit = {
//    MemoryAccess.setByteAtOffset(segment, offset, v)
//  }
//  @inline
//  final def getShort(segment: MemorySegment, offset: Long): Short = {
//    MemoryAccess.getShortAtOffset(segment, offset, ByteOrder.BIG_ENDIAN)
//  }
//  @inline
//  final def setShort(v: Short, segment: MemorySegment, offset: Long): Unit = {
//      MemoryAccess.setShortAtOffset(segment, offset, ByteOrder.BIG_ENDIAN, v)
//  }

  @inline
  def getBytes(src: MemorySegment, srcOffset: Long, dst: Array[Byte], size: Int): Unit = {
    val srcImpl = src.asInstanceOf[AbstractMemorySegmentImpl]

    UNSAFE.copyMemory(
      srcImpl.unsafeGetBase(),
      srcImpl.unsafeGetOffset() + srcOffset,
      dst,
      Unsafe.ARRAY_BYTE_BASE_OFFSET,
      size
    );
  }

//  import jdk.incubator.foreign.MemoryHandles
//
//  final val ARR_HANDLE: VarHandle = MethodHandles.byteArrayViewVarHandle(classOf[Array[Long]], ByteOrder.nativeOrder())
//  final val MEM_HANDLE: VarHandle = MemoryHandles.varHandle(classOf[Long], 1, ByteOrder.nativeOrder())

//  @inline
//  def getBytes(src: MemorySegment, srcOffset: Long, dst: Array[Byte], size: Int): Unit = {
//
//    var i = 0
//    val ls = size / 8 * 8
//    while (i < ls) {
//      val v: Long = getLongNative(src, srcOffset + i) // MEM_HANDLE.get(src, srcOffset + i)
//      ARR_HANDLE.set(dst, i, v)
//      i += 8
//    }
//
//    while (i < size) {
//      val v = getByte(src, srcOffset + i)
//      dst(i) = v
//      i += 1
//    }
//  }

  @inline
  def getBytes(src: MemorySegment, srcOffset: Long, size: Int): Array[Byte] = {
    val a = Array.ofDim[Byte](size)
    getBytes(src, srcOffset, a, size)
    a
  }

  @inline
  def setBytes(src: Array[Byte], srcOffset: Int, dst: MemorySegment, dstOffset: Long, size: Int): Unit = {
    val dstImpl = dst.asInstanceOf[AbstractMemorySegmentImpl]

    UNSAFE.copyMemory(
      src,
      Unsafe.ARRAY_BYTE_BASE_OFFSET + srcOffset,
      dstImpl.unsafeGetBase(),
      dstImpl.unsafeGetOffset() + dstOffset,
      size
    )
  }

  @inline
  def copyBytes(src: MemorySegment, srcOffset: Long, dst: MemorySegment, dstOffset: Long, size: Int): Unit = {
    var i = 0
    while (i < size) {
      val b = MemoryAccess.getByteAtOffset(src, srcOffset + i)
      MemoryAccess.setByteAtOffset(dst, dstOffset + i, b)
      i += 1
    }
  }

  def loadRowKey(
      byteBuffer: ByteBuffer,
      dimensions: Array[Dimension],
      internalRowBuilder: InternalRowBuilder
  ): Unit = {

    val baseTime = byteBuffer.getLong()

    var i = 0

    dimensions.foreach { dim =>

      val value = dim.rStorable.read(byteBuffer)
      if (dim.isInstanceOf[RawDimension[_]]) {
        internalRowBuilder.set((Table.DIM_TAG_OFFSET + i).toByte, value)
      }
      i += 1
    }
    val currentTime = byteBuffer.getLong
    internalRowBuilder.set(Time(baseTime + currentTime))
  }

  def loadValue(byteBuffer: ByteBuffer, internalRowBuilder: InternalRowBuilder) = {}

  def loadRow(byteBuffer: ByteBuffer, dimensions: Array[Dimension], internalRowBuilder: InternalRowBuilder): Unit = {
    val size = byteBuffer.getInt()
    val p = byteBuffer.position()
    loadRowKey(byteBuffer, dimensions, internalRowBuilder)
    loadValue(byteBuffer, internalRowBuilder)
    if (byteBuffer.position() - p != size) throw new IllegalStateException("Wrong row format")
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

  val BYTES_COMPARATOR: Comparator[Array[Byte]] = new Comparator[Array[Byte]] {
    override def compare(o1: Array[Byte], o2: Array[Byte]): Int = {
      StorageFormat.compareTo(o1, 0, o2, 0, math.max(o1.length, o2.length))
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

    // Short circuit equal case
    if ((a1 eq a2) && offset1 == offset2) return 0

    val stride = 8
    val strideLimit = size & ~(stride - 1)
    val offset1Adj = offset1 + BYTE_ARRAY_BASE_OFFSET
    val offset2Adj = offset2 + BYTE_ARRAY_BASE_OFFSET
    var i = 0
    /*
     * Compare 8 bytes at a time. Benchmarking on x86 shows a stride of 8 bytes is no slower
     * than 4 bytes even on 32-bit. On the other hand, it is substantially faster on 64-bit.
     */
    i = 0
    while (i < strideLimit) {
      val lw = UNSAFE.getLong(a1, offset1Adj + i)
      val rw = UNSAFE.getLong(a2, offset2Adj + i)
      if (lw != rw) {
        val n: Int = java.lang.Long.numberOfTrailingZeros(lw ^ rw) & ~0x7
        return (((lw >>> n) & 0xFF).toInt) - (((rw >>> n) & 0xFF).toInt)
      }
      i += stride
    }
    // The epilogue to cover the last (minLength % stride) elements.

    while (i < size) {
      val a = a1(offset1 + i) & 0xFF
      val b = a2(offset2 + i) & 0xFF
      if (a != b) return a - b
      i += 1
    }
    0
  }

  def incByteArray(splitPoint: Array[Byte], size: Int): Array[Byte] = {
    val t = (BigInt(1, splitPoint) + 1).toByteArray
    val a = Array.ofDim[Byte](size)
    Array.copy(t, 0, a, size - t.length, t.length)
    a
  }

}
