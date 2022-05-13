package org.yupana.khipu

import jdk.incubator.foreign.{ MemoryAccess, MemorySegment }
import org.yupana.api.Time
import org.yupana.api.schema.{ Dimension, RawDimension, Table }
import org.yupana.core.model.InternalRowBuilder

import java.nio.{ ByteBuffer, ByteOrder }
import java.util.Comparator

object StorageFormat {

  @inline
  def getInt(segment: MemorySegment, offset: Long): Int = {
    MemoryAccess.getIntAtOffset(segment, offset, ByteOrder.BIG_ENDIAN)
  }

  @inline
  final def setInt(v: Int, segment: MemorySegment, offset: Long): Unit = {
    MemoryAccess.setIntAtOffset(segment, offset, ByteOrder.BIG_ENDIAN, v)
  }

  @inline
  final def getLong(segment: MemorySegment, offset: Long): Long = {
    MemoryAccess.getLongAtOffset(segment, offset, ByteOrder.BIG_ENDIAN)
  }

  @inline
  final def getByte(segment: MemorySegment, offset: Long): Byte = {
    MemoryAccess.getByteAtOffset(segment, offset)
  }

  @inline
  final def setByte(v: Byte, segment: MemorySegment, offset: Long): Unit = {
    MemoryAccess.setByteAtOffset(segment, offset, v)
  }

  @inline
  final def getShort(segment: MemorySegment, offset: Long): Short = {
    MemoryAccess.getShortAtOffset(segment, offset, ByteOrder.BIG_ENDIAN)
  }

  @inline
  final def setShort(v: Short, segment: MemorySegment, offset: Long): Unit = {
    MemoryAccess.setShortAtOffset(segment, offset, ByteOrder.BIG_ENDIAN, v)
  }

  @inline
  def getBytes(src: MemorySegment, srcOffset: Long, dst: Array[Byte], dstOffset: Int, size: Int): Unit = {
    var i = 0
    while (i < size) {
      val b = MemoryAccess.getByteAtOffset(src, srcOffset + i)
      dst(dstOffset + i) = b
      i += 1
    }
  }

  @inline
  def getBytes(src: MemorySegment, srcOffset: Long, size: Int): Array[Byte] = {
    val a = Array.ofDim[Byte](size)
    getBytes(src, srcOffset, a, 0, size)
    a
  }

  @inline
  def setBytes(src: Array[Byte], srcOffset: Int, dst: MemorySegment, dstOffset: Long, size: Int): Unit = {
    var i = 0
    while (i < size) {
      val b = src(srcOffset + i)
      MemoryAccess.setByteAtOffset(dst, dstOffset + i, b)
      i += 1
    }
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

      while (commonPrefix < stepsLimit &&
             getLong(buf, commonPrefix) == getLong(prefix, commonPrefix)) {
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

  def compareTo(a1: Array[Byte], offset1: Int, a2: Array[Byte], offset2: Int, size: Int): Int = {
    if (a1 eq a2) {
      0
    } else {

      val n = math.min(a1.length, size)
      val m = math.min(a2.length, size)
      var i = 0
      var j = 0
      while (i < n && j < m) {
        val x = a1(i) & 0xff
        val y = a2(j) & 0xff
        if (x != y) return x - y

        i += 1
        j += 1
      }
      n - m
    }
  }

}
