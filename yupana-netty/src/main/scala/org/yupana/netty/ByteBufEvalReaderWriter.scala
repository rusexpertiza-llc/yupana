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

package org.yupana.netty

import io.netty.buffer.ByteBuf
import org.threeten.extra.PeriodDuration
import org.yupana.api.types.{ ByteReaderWriter, ID }
import org.yupana.api.{ Blob, Currency, Time }

import java.math.BigInteger
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

object ByteBufEvalReaderWriter extends ByteReaderWriter[ByteBuf] with Serializable {

  override def sizeOfBoolean: Int = 1

  override def readBoolean(b: ByteBuf): Boolean = {
    b.readBoolean()
  }

  override def readBoolean(b: ByteBuf, offset: Int): Boolean = {
    b.getBoolean(offset)
  }
  override def writeBoolean(b: ByteBuf, v: Boolean): Int = {
    b.writeBoolean(v)
    1
  }

  override def writeBoolean(b: ByteBuf, offset: Int, v: Boolean): Int = {
    b.setBoolean(offset, v)
    1
  }

  override def readBytes(b: ByteBuf, d: ID[Array[Byte]]): ID[Unit] = {
    b.readBytes(d)
  }

  override def readBytes(b: ByteBuf, offset: Int, d: ID[Array[Byte]]): ID[Unit] = {
    b.getBytes(offset, d)
  }

  override def writeBytes(b: ByteBuf, v: ID[Array[Byte]]): Int = {
    b.writeBytes(v)
    v.length
  }

  override def writeBytes(b: ByteBuf, offset: Int, v: ID[Array[Byte]]): Int = {
    b.setBytes(offset, v)
    v.length
  }

  override def sizeOfInt: Int = 4

  override def readInt(b: ByteBuf): Int = {
    b.readInt()
  }

  override def readInt(b: ByteBuf, offset: Int): Int = {
    b.getInt(offset)
  }

  override def writeInt(b: ByteBuf, v: Int): Int = {
    b.writeInt(v)
    4
  }

  override def writeInt(b: ByteBuf, offset: Int, v: Int): Int = {
    b.setInt(offset, v)
    4
  }

  override def sizeOfLong: Int = 8

  override def readLong(b: ByteBuf): Long = {
    b.readLong()
  }

  override def readLong(b: ByteBuf, offset: Int): Long = {
    b.getLong(offset)
  }

  override def writeLong(b: ByteBuf, v: Long): Int = {
    b.writeLong(v)
    8
  }

  override def writeLong(b: ByteBuf, offset: Int, v: Long): Int = {
    b.setLong(offset, v)
    8
  }

  override def sizeOfDouble: Int = 8

  override def readDouble(b: ByteBuf): Double = {
    b.readDouble()
  }

  override def readDouble(b: ByteBuf, offset: Int): Double = {
    b.getDouble(offset)
  }

  override def writeDouble(b: ByteBuf, v: Double): Int = {
    b.writeDouble(v)
    8
  }

  override def writeDouble(b: ByteBuf, offset: Int, v: Double): Int = {
    b.setDouble(offset, v)
    8
  }

  override def sizeOfShort: Int = 2

  override def readShort(b: ByteBuf): Short = {
    b.readShort()
  }

  override def readShort(b: ByteBuf, offset: Int): Short = {
    b.getShort(offset)
  }

  override def writeShort(b: ByteBuf, v: Short): Int = {
    b.writeShort(v)
    2
  }

  override def writeShort(b: ByteBuf, offset: Int, v: Short): Int = {
    b.setShort(offset, v)
    2
  }

  override def sizeOfByte: Int = 1

  override def readByte(b: ByteBuf): Byte = {
    b.readByte()
  }

  override def readByte(b: ByteBuf, offset: Int): Byte = {
    b.getByte(offset)
  }
  override def writeByte(b: ByteBuf, v: Byte): Int = {
    b.writeByte(v)
    1
  }

  override def writeByte(b: ByteBuf, offset: Int, v: Byte): Int = {
    b.setByte(offset, v)
    1
  }

  override def sizeOfTime: Int = 8

  override def readTime(b: ByteBuf): Time = {
    Time(b.readLong())
  }

  override def readTime(b: ByteBuf, offset: Int): Time = {
    Time(b.getLong(offset))
  }

  override def writeTime(b: ByteBuf, v: Time): Int = {
    b.writeLong(v.millis)
    8
  }

  override def writeTime(b: ByteBuf, offset: Int, v: Time): Int = {
    b.setLong(offset, v.millis)
    8
  }

  override def sizeOfTuple[T, U](v: (T, U), tSize: ID[T] => Int, uSize: ID[U] => Int): Int = {
    tSize(v._1) + uSize(v._2)
  }

  override def readTuple[T, U](b: ByteBuf, tReader: ByteBuf => T, uReader: ByteBuf => U): (T, U) = {
    (tReader(b), uReader(b))
  }
  override def readTuple[T, U](
      b: ByteBuf,
      offset: Int,
      tReader: ByteBuf => T,
      uReader: ByteBuf => U
  ): (T, U) = {
    val bb = b.slice(offset, b.capacity() - offset)
    (tReader(bb), uReader(bb))
  }

  override def writeTuple[T, U](
      b: ByteBuf,
      v: (T, U),
      tWrite: (ByteBuf, T) => Int,
      uWrite: (ByteBuf, U) => Int
  ): Int = {
    tWrite(b, v._1) + uWrite(b, v._2)
  }

  override def writeTuple[T, U](
      b: ByteBuf,
      offset: Int,
      v: (T, U),
      tWrite: (ByteBuf, T) => Int,
      uWrite: (ByteBuf, U) => Int
  ): Int = {
    val bb = b.slice(offset, b.capacity() - offset)
    bb.writerIndex(0)
    tWrite(bb, v._1) + uWrite(bb, v._2)
  }

  override def sizeOfString(v: String): Int = 4 + v.length

  override def readString(b: ByteBuf): String = {
    val length = b.readInt()
    val bytes = Array.ofDim[Byte](length)
    b.readBytes(bytes)
    new String(bytes, StandardCharsets.UTF_8)
  }

  override def readString(b: ByteBuf, offset: Int): String = {
    val length = b.getInt(offset)
    val bytes = Array.ofDim[Byte](length)
    b.getBytes(offset + 4, bytes)
    new String(bytes, StandardCharsets.UTF_8)
  }

  override def writeString(b: ByteBuf, v: String): Int = {
    val a = v.getBytes(StandardCharsets.UTF_8)
    b.writeInt(a.length).writeBytes(a)
    a.length + 4
  }

  override def writeString(b: ByteBuf, offset: Int, v: String): Int = {
    val a = v.getBytes(StandardCharsets.UTF_8)
    b.setInt(offset, a.length)
    b.setBytes(offset + 4, a)
    a.length + 4
  }

  def sizeOfVlong(v: Long) = {
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
      len + 1
    }
  }

  override def readVLong(bb: ByteBuf, offset: Int): Long = {
    val first = bb.getByte(offset)

    if (first >= -112) return first

    val len = if (first >= -120) {
      -111 - first
    } else {
      -119 - first
    }

    var result = 0L
    var i = 1
    while (i < len) {
      val b = bb.getByte(i)
      result <<= 8
      result |= (b & 0xFF)
      i += 1
    }

    if (first >= -120) result else result ^ -1L

  }

  override def readVLong(bb: ByteBuf): Long = {
    val first = bb.readByte()

    if (first >= -112) return first

    val len = if (first >= -120) {
      -111 - first
    } else {
      -119 - first
    }

    var result = 0L
    var i = 1
    while (i < len) {
      val b = bb.readByte()
      result <<= 8
      result |= (b & 0xFF)
      i += 1
    }

    if (first >= -120) result else result ^ -1L
  }

  override def writeVLong(bb: ByteBuf, v: Long): Int = {
    if (v <= 127 && v > -112) {
      bb.writeByte(v.toByte)
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

      bb.writeByte(len.toByte)

      len = if (len < -120) {
        -(len + 120)
      } else {
        -(len + 112)
      }

      var idx = len - 1
      while (idx >= 0) {
        val shift = idx * 8
        val mask = 0xFFL << shift
        bb.writeByte(((ll & mask) >> shift).toByte)
        idx -= 1
      }
      len + 1
    }
  }

  override def writeVLong(bb: ByteBuf, offset: Int, v: Long): Int = {
    if (v <= 127 && v > -112) {
      bb.setByte(offset, v.toByte)
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

      bb.setByte(offset, len.toByte)

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
        bb.setByte(offset + i + 1, ((ll & mask) >> shift).toByte)
        idx -= 1
        i += 1
      }
      len + 1
    }
  }

  override def readVInt(b: ByteBuf): Int = {
    val l = readVLong(b)
    if (l <= Int.MaxValue && l >= Int.MinValue) l.toInt
    else throw new IllegalArgumentException("Got Long but Int expected")
  }

  override def readVInt(b: ByteBuf, offset: Int): Int = {
    val l = readVLong(b, offset)
    if (l <= Int.MaxValue && l >= Int.MinValue) l.toInt
    else throw new IllegalArgumentException("Got Long but Int expected")
  }

  override def writeVInt(b: ByteBuf, v: Int): Int = {
    writeVLong(b, v)
  }

  override def writeVInt(b: ByteBuf, offset: Int, v: Int): Int = {
    writeVLong(b, offset, v)
  }

  override def readVShort(b: ByteBuf): Short = {
    val l = readVLong(b)
    if (l <= Short.MaxValue && l >= Short.MinValue) l.toShort
    else throw new IllegalArgumentException("Got Long but Short expected")
  }

  override def readVShort(b: ByteBuf, offset: Int): Short = {
    val l = readVLong(b, offset)
    if (l <= Short.MaxValue && l >= Short.MinValue) l.toShort
    else throw new IllegalArgumentException("Got Long but Short expected")
  }

  override def writeVShort(b: ByteBuf, v: Short): Int = {
    writeVLong(b, v)
  }
  override def writeVShort(b: ByteBuf, offset: Int, v: Short): Int = {
    writeVLong(b, offset, v)
  }

  override def sizeOfBigDecimal(v: BigDecimal): Int = {
    val u = v.underlying()
    val a = u.unscaledValue().toByteArray
    sizeOfVlong(u.scale()) + sizeOfVlong(a.length) + a.length
  }

  override def readBigDecimal(b: ByteBuf): BigDecimal = {
    val scale = readVInt(b)
    val size = readVInt(b)
    val bytes = Array.ofDim[Byte](size)
    b.readBytes(bytes)
    new java.math.BigDecimal(new BigInteger(bytes), scale)
  }

  override def readBigDecimal(b: ByteBuf, offset: Int): BigDecimal = {
    val scale = readVInt(b, offset)
    val sScale = sizeOfVlong(scale)
    val size = readVInt(b, offset + sScale)
    val sSize = sizeOfVlong(size)
    val bytes = Array.ofDim[Byte](size)
    b.getBytes(offset + sScale + sSize, bytes)
    new java.math.BigDecimal(new BigInteger(bytes), scale)
  }

  override def writeBigDecimal(b: ByteBuf, v: BigDecimal): Int = {
    val u = v.underlying()
    val a = u.unscaledValue().toByteArray
    val s1 = writeVLong(b, u.scale())
    val s2 = writeVLong(b, a.length)
    b.writeBytes(a)
    s1 + s2 + a.length
  }

  override def writeBigDecimal(b: ByteBuf, offset: Int, v: BigDecimal): Int = {
    val u = v.underlying()
    val a = u.unscaledValue().toByteArray
    val s1 = writeVLong(b, offset, u.scale())
    val s2 = writeVLong(b, offset + s1, a.length)
    b.setBytes(offset + s1 + s2, a)
    s1 + s2 + a.length
  }

  override def sizeOfSeq[T](v: Seq[T], size: ID[T] => Int): Int = {
    v.foldLeft(sizeOfVlong(v.size)) { (s, vv) =>
      s + size(vv)
    }
  }

  override def readSeq[T: ClassTag](b: ByteBuf, reader: ByteBuf => T): Seq[T] = {
    val size = readVInt(b)
    val result = ListBuffer.empty[T]

    for (_ <- 0 until size) {
      result += reader(b)
    }

    result.toSeq
  }

  override def readSeq[T: ClassTag](b: ByteBuf, offset: Int, reader: ByteBuf => T): Seq[T] = {
    val slice = b.slice(offset, b.capacity() - offset)

    val size = readVInt(slice)
    val result = ListBuffer.empty[T]

    for (_ <- 0 until size) {
      result += reader(slice)
    }
    result.toSeq
  }

  override def writeSeq[T](b: ByteBuf, seq: Seq[T], writer: (ByteBuf, T) => Int)(
      implicit ct: ClassTag[T]
  ): Int = {
    val s1 = writeVInt(b, seq.size)
    seq.foldLeft(s1)((s, v) => s + writer(b, v))
  }

  override def writeSeq[T](b: ByteBuf, offset: Int, seq: Seq[T], writer: (ByteBuf, T) => Int)(
      implicit ct: ClassTag[T]
  ): Int = {
    val slice = b.slice(offset, b.capacity() - offset)
    slice.writerIndex(0)
    val s1 = writeVInt(slice, seq.size)
    seq.foldLeft(s1)((s, v) => s + writer(slice, v))
  }

  override def sizeOfBlob(v: Blob): Int = {
    sizeOfVlong(v.bytes.length) + v.bytes.length
  }

  override def readBlob(b: ByteBuf): Blob = {
    val size = readVInt(b)
    val data = new Array[Byte](size)
    b.readBytes(data)
    Blob(data)
  }

  override def readBlob(b: ByteBuf, offset: Int): Blob = {
    val size = readVInt(b, offset)
    val p = sizeOfVlong(size)
    val data = new Array[Byte](size)
    b.getBytes(offset + p, data)
    Blob(data)
  }

  override def writeBlob(b: ByteBuf, v: Blob): Int = {
    val s = writeVInt(b, v.bytes.length)
    b.writeBytes(v.bytes)
    s + v.bytes.length
  }

  override def writeBlob(b: ByteBuf, offset: Int, v: Blob): Int = {
    val s = writeVInt(b, offset, v.bytes.length)
    b.setBytes(offset + s, v.bytes)
    s + v.bytes.length
  }

  override def readVTime(b: ByteBuf): Time = {
    Time(readVLong(b))
  }

  override def readVTime(b: ByteBuf, offset: Int): Time = {
    Time(readVLong(b, offset))
  }
  override def writeVTime(b: ByteBuf, v: Time): Int = {
    writeVLong(b, v.millis)
  }

  override def writeVTime(b: ByteBuf, offset: Int, v: Time): Int = {
    writeVLong(b, offset, v.millis)
  }

  override def sizeOfPeriodDuration(v: PeriodDuration): Int = {
    sizeOfString(v.toString)
  }

  override def readPeriodDuration(b: ByteBuf): PeriodDuration = {
    val s = readString(b)
    PeriodDuration.parse(s)
  }

  override def readPeriodDuration(b: ByteBuf, offset: Int): PeriodDuration = {
    val s = readString(b, offset)
    PeriodDuration.parse(s)
  }

  override def writePeriodDuration(b: ByteBuf, v: PeriodDuration): Int = {
    writeString(b, v.toString)
  }

  override def writePeriodDuration(b: ByteBuf, offset: Int, v: PeriodDuration): Int = {
    writeString(b, offset, v.toString)
  }

  override def sizeOfCurrency: Int = 8

  override def readCurrency(b: ByteBuf): ID[Currency] = Currency(readLong(b))
  override def readCurrency(b: ByteBuf, offset: Int): ID[Currency] = Currency(readLong(b, offset))

  override def writeCurrency(b: ByteBuf, v: ID[Currency]): Int = writeLong(b, v.value)
  override def writeCurrency(b: ByteBuf, offset: Int, v: ID[Currency]): Int = writeLong(b, offset, v.value)

  override def readVCurrency(b: ByteBuf): ID[Currency] = Currency(readVLong(b))
  override def readVCurrency(b: ByteBuf, offset: Int): ID[Currency] = Currency(readVLong(b, offset))

  override def writeVCurrency(b: ByteBuf, v: ID[Currency]): Int = writeVLong(b, v.value)
  override def writeVCurrency(b: ByteBuf, offset: Int, v: ID[Currency]): Int = writeVLong(b, offset, v.value)
}
