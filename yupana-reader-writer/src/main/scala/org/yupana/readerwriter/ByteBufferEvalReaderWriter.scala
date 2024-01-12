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

package org.yupana.readerwriter

import org.threeten.extra.PeriodDuration
import org.yupana.api.{ Blob, Time }
import org.yupana.api.types.ReaderWriter

import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

object ByteBufferEvalReaderWriter extends ReaderWriter[ByteBuffer, ID, TypedInt] {

  override def readBoolean(b: ByteBuffer): Boolean = {
    b.get() != 0
  }

  override def readBoolean(b: ByteBuffer, offset: Int): Boolean = {
    b.get(offset) != 0
  }
  override def writeBoolean(b: ByteBuffer, v: Boolean): Int = {
    val x = if (v) 1 else 0
    b.put(x.toByte)
    1
  }

  override def writeBoolean(b: ByteBuffer, offset: Int, v: Boolean): Int = {
    val x = if (v) 1 else 0
    b.put(offset, x.toByte)
    1
  }

  override def readInt(b: ByteBuffer): Int = {
    b.getInt()
  }

  override def readInt(b: ByteBuffer, offset: Int): Int = {
    b.getInt(offset)
  }

  override def writeInt(b: ByteBuffer, v: Int): Int = {
    b.putInt(v)
    4
  }

  override def writeInt(b: ByteBuffer, offset: Int, v: Int): Int = {
    b.putInt(offset, v)
    4
  }
  override def readLong(b: ByteBuffer): Long = {
    b.getLong()
  }

  override def readLong(b: ByteBuffer, offset: Int): Long = {
    b.getLong(offset)
  }

  override def writeLong(b: ByteBuffer, v: Long): Int = {
    b.putLong(v)
    8
  }

  override def writeLong(b: ByteBuffer, offset: Int, v: Long): Int = {
    b.putLong(offset, v)
    8
  }

  override def readDouble(b: ByteBuffer): Double = {
    b.getDouble()
  }

  override def readDouble(b: ByteBuffer, offset: Int): Double = {
    b.getDouble(offset)
  }

  override def writeDouble(b: ByteBuffer, v: Double): Int = {
    b.putDouble(v)
    8
  }

  override def writeDouble(b: ByteBuffer, offset: Int, v: Double): Int = {
    b.putDouble(offset, v)
    8
  }
  override def readShort(b: ByteBuffer): Short = {
    b.getShort()
  }

  override def readShort(b: ByteBuffer, offset: Int): Short = {
    b.getShort(offset)
  }

  override def writeShort(b: ByteBuffer, v: Short): Int = {
    b.putShort(v)
    2
  }

  override def writeShort(b: ByteBuffer, offset: Int, v: Short): Int = {
    b.putShort(offset, v)
    2
  }

  override def readByte(b: ByteBuffer): Byte = {
    b.get()
  }

  override def readByte(b: ByteBuffer, offset: Int): Byte = {
    b.get(offset)
  }
  override def writeByte(b: ByteBuffer, v: Byte): Int = {
    b.put(v)
    1
  }

  override def writeByte(b: ByteBuffer, offset: Int, v: Byte): Int = {
    b.put(offset, v)
    1
  }

  override def readTime(b: ByteBuffer): Time = {
    Time(b.getLong())
  }

  override def readTime(b: ByteBuffer, offset: Int): Time = {
    Time(b.getLong(offset))
  }

  override def writeTime(b: ByteBuffer, v: Time): Int = {
    b.putLong(v.millis)
    8
  }

  override def writeTime(b: ByteBuffer, offset: Int, v: Time): Int = {
    b.putLong(v.millis)
    8
  }

  override def readTuple[T, U](b: ByteBuffer, tReader: ByteBuffer => T, uReader: ByteBuffer => U): (T, U) = {
    (tReader(b), uReader(b))
  }
  override def readTuple[T, U](
      b: ByteBuffer,
      offset: Int,
      tReader: ByteBuffer => T,
      uReader: ByteBuffer => U
  ): (T, U) = {
    val bb = b.slice(offset, b.limit() - offset)
    (tReader(bb), uReader(bb))
  }

  override def writeTuple[T, U](
      b: ByteBuffer,
      v: (T, U),
      tWrite: (ByteBuffer, T) => Int,
      uWrite: (ByteBuffer, U) => Int
  ): Int = {
    tWrite(b, v._1) + uWrite(b, v._2)
  }

  override def writeTuple[T, U](
      b: ByteBuffer,
      offset: Int,
      v: (T, U),
      tWrite: (ByteBuffer, T) => Int,
      uWrite: (ByteBuffer, U) => Int
  ): Int = {
    val bb = b.slice(offset, b.limit() - offset)
    tWrite(bb, v._1) + uWrite(bb, v._2)
  }

  override def readString(b: ByteBuffer): String = {
    val length = b.getInt()
    val bytes = Array.ofDim[Byte](length)
    b.get(bytes)
    new String(bytes, StandardCharsets.UTF_8)
  }

  override def readString(b: ByteBuffer, offset: Int): String = {
    val length = b.getInt()
    val bytes = Array.ofDim[Byte](length)
    b.get(offset, bytes)
    new String(bytes, StandardCharsets.UTF_8)
  }

  override def writeString(b: ByteBuffer, v: String): Int = {
    val a = v.getBytes(StandardCharsets.UTF_8)
    b.putInt(a.length).put(a)
    a.length
  }

  override def writeString(b: ByteBuffer, offset: Int, v: String): Int = {
    val a = v.getBytes(StandardCharsets.UTF_8)
    b.putInt(offset, a.length)
    b.put(offset + 4, a)
    a.length
  }

  override def readVLong(bb: ByteBuffer, offset: Int): Long = {
    val first = bb.get(offset)

    if (first >= -112) return first

    val len = if (first >= -120) {
      -111 - first
    } else {
      -119 - first
    }

    var result = 0L
    var i = 1
    while (i < len) {
      val b = bb.get(i)
      result <<= 8
      result |= (b & 0xFF)
      i += 1
    }

    if (first >= -120) result else result ^ -1L

  }

  override def readVLong(bb: ByteBuffer): Long = {
    val first = bb.get()

    if (first >= -112) return first

    val len = if (first >= -120) {
      -111 - first
    } else {
      -119 - first
    }

    var result = 0L
    var i = 1
    while (i < len) {
      val b = bb.get()
      result <<= 8
      result |= (b & 0xFF)
      i += 1
    }

    if (first >= -120) result else result ^ -1L
  }

  override def writeVLong(bb: ByteBuffer, v: Long): Int = {
    if (v <= 127 && v > -112) {
      bb.put(v.toByte)
      1
    } else {
      var ll = v
      var len = -112

      if (ll < 0) {
        len = -120
        ll ^= -1L
      }

      var tmp = ll
      while (tmp != 0) {
        tmp >>= 8
        len -= 1
      }

      bb.put(len.toByte)

      len = if (len < -120) {
        -(len + 120)
      } else {
        -(len + 112)
      }

      var idx = len - 1
      while (idx >= 0) {
        val shift = idx * 8
        val mask = 0xFFL << shift
        bb.put(((ll & mask) >> shift).toByte)
        idx -= 1
      }
      len
    }
  }

  def vLongSize(v: Long) = {
    if (v <= 127 && v > -112) {
      1
    } else {
      var ll = v
      var len = -112

      if (ll < 0) {
        len = -120
        ll ^= -1L
      }

      var tmp = ll
      while (tmp != 0) {
        tmp >>= 8
        len -= 1
      }

      len = if (len < -120) {
        -(len + 120)
      } else {
        -(len + 112)
      }
      len
    }
  }

  override def writeVLong(bb: ByteBuffer, offset: Int, v: Long): Int = {
    if (v <= 127 && v > -112) {
      bb.put(offset, v.toByte)
      1
    } else {
      var ll = v
      var len = -112

      if (ll < 0) {
        len = -120
        ll ^= -1L
      }

      var tmp = ll
      while (tmp != 0) {
        tmp >>= 8
        len -= 1
      }

      bb.put(offset + 1, len.toByte)

      len = if (len < -120) {
        -(len + 120)
      } else {
        -(len + 112)
      }

      var idx = len - 1
      var i = 0
      while (idx >= 0) {
        val shift = idx * 8
        val mask = 0xFFL << shift
        bb.put(offset + i + 2, ((ll & mask) >> shift).toByte)
        idx -= 1
        i += i
      }
      len
    }
  }

  override def readVInt(b: ByteBuffer): Int = {
    val l = readVLong(b)
    if (l <= Int.MaxValue && l >= Int.MinValue) l.toInt
    else throw new IllegalArgumentException("Got Long but Int expected")
  }

  override def readVInt(b: ByteBuffer, offset: Int): Int = {
    val l = readVLong(b, offset)
    if (l <= Int.MaxValue && l >= Int.MinValue) l.toInt
    else throw new IllegalArgumentException("Got Long but Int expected")
  }

  override def writeVInt(b: ByteBuffer, v: Int): Int = {
    writeVLong(b, v)
    v
  }

  override def writeVInt(b: ByteBuffer, offset: Int, v: Int): Int = {
    writeVLong(b, offset, v)
    v
  }

  override def readVShort(b: ByteBuffer): Short = {
    val l = readVLong(b)
    if (l <= Short.MaxValue && l >= Short.MinValue) l.toShort
    else throw new IllegalArgumentException("Got Long but Short expected")
  }

  override def readVShort(b: ByteBuffer, offset: Int): Short = {
    val l = readVLong(b, offset)
    if (l <= Short.MaxValue && l >= Short.MinValue) l.toShort
    else throw new IllegalArgumentException("Got Long but Short expected")
  }

  override def writeVShort(b: ByteBuffer, v: Short): Int = {
    writeVLong(b, v)
  }
  override def writeVShort(b: ByteBuffer, offset: Int, v: Short): Int = {
    writeVLong(b, offset, v)
  }

  override def readBigDecimal(b: ByteBuffer): BigDecimal = {
    val scale = readVInt(b)
    val size = readVInt(b)
    val bytes = Array.ofDim[Byte](size)
    b.get(bytes)
    new java.math.BigDecimal(new BigInteger(bytes), scale)
  }

  override def readBigDecimal(b: ByteBuffer, offset: Int): BigDecimal = {
    val scale = readVInt(b, offset)
    val sScale = vLongSize(scale)
    val size = readVInt(b, offset + sScale)
    val sSize = vLongSize(size)
    val bytes = Array.ofDim[Byte](size)
    b.get(offset + sScale + sSize, bytes)
    new java.math.BigDecimal(new BigInteger(bytes), scale)
  }

  override def writeBigDecimal(b: ByteBuffer, v: BigDecimal): Int = {
    val u = v.underlying()
    val a = u.unscaledValue().toByteArray
    val s1 = writeVLong(b, u.scale())
    val s2 = writeVLong(b, a.length)
    b.put(a)
    s1 + s2 + a.length
  }

  override def writeBigDecimal(b: ByteBuffer, offset: Int, v: BigDecimal): Int = {
    val u = v.underlying()
    val a = u.unscaledValue().toByteArray
    val s1 = writeVLong(b, offset, u.scale())
    val s2 = writeVLong(b, offset + s1, a.length)
    b.put(offset + s1 + s2, a)
    s1 + s2 + a.length
  }

  override def readSeq[T: ClassTag](b: ByteBuffer, reader: ByteBuffer => T): Seq[T] = {
    val size = readVInt(b)
    val result = ListBuffer.empty[T]

    for (_ <- 0 until size) {
      result += reader(b)
    }

    result.toSeq
  }

  override def readSeq[T: ClassTag](b: ByteBuffer, offset: Int, reader: ByteBuffer => T): Seq[T] = {
    val p = b.position()
    b.position(offset)
    val size = readVInt(b)
    val result = ListBuffer.empty[T]

    for (_ <- 0 until size) {
      result += reader(b)
    }
    b.position(p)
    result.toSeq
  }

  override def writeSeq[T](b: ByteBuffer, seq: Seq[T], writer: (ByteBuffer, T) => Int)(
      implicit ct: ClassTag[T]
  ): Int = {
    val s1 = writeVInt(b, seq.size)
    seq.foldLeft(s1)((s, v) => s + writer(b, v))
  }

  override def writeSeq[T](b: ByteBuffer, offset: Int, seq: Seq[T], writer: (ByteBuffer, T) => Int)(
      implicit ct: ClassTag[T]
  ): Int = {
    val p = b.position()
    b.position(offset)
    val s1 = writeVInt(b, offset, seq.size)
    val s = seq.foldLeft(s1)((s, v) => s + writer(b, v))
    b.position(p)
    s
  }

  override def readBlob(b: ByteBuffer): Blob = {
    val size = readVInt(b)
    val data = new Array[Byte](size)
    b.get(data)
    Blob(data)
  }

  override def readBlob(b: ByteBuffer, offset: Int): Blob = {
    val p = b.position()
    b.position(offset)
    val size = readVInt(b)
    val data = new Array[Byte](size)
    b.get(data)
    b.position(p)
    Blob(data)
  }

  override def writeBlob(b: ByteBuffer, v: Blob): Int = {
    val s = writeVInt(b, v.bytes.length)
    b.put(v.bytes)
    s + v.bytes.length
  }

  override def writeBlob(b: ByteBuffer, offset: Int, v: Blob): Int = {
    val s = writeVInt(b, offset, v.bytes.length)
    b.put(offset + s, v.bytes)
    s + v.bytes.length
  }

  override def readVTime(b: ByteBuffer): Time = {
    Time(readVLong(b))
  }

  override def readVTime(b: ByteBuffer, offset: Int): Time = {
    Time(readVLong(b, offset))
  }
  override def writeVTime(b: ByteBuffer, v: Time): Int = {
    writeVLong(b, v.millis)
  }

  override def writeVTime(b: ByteBuffer, offset: Int, v: Time): Int = {
    writeVLong(b, offset, v.millis)
  }

  override def readPeriodDuration(b: ByteBuffer): PeriodDuration = {
    val s = readString(b)
    PeriodDuration.parse(s)
  }

  override def readPeriodDuration(b: ByteBuffer, offset: Int): PeriodDuration = {
    val s = readString(b, offset)
    PeriodDuration.parse(s)
  }

  override def writePeriodDuration(b: ByteBuffer, v: PeriodDuration): Int = {
    writeString(b, v.toString)
  }

  override def writePeriodDuration(b: ByteBuffer, offset: Int, v: PeriodDuration): Int = {
    writeString(b, offset, v.toString)
  }

}
