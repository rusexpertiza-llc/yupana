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

package org.yupana.postgres.protocol

import io.netty.buffer.ByteBuf
import org.threeten.extra.PeriodDuration
import org.yupana.api.types.{ ByteReaderWriter, ID, TypedInt }
import org.yupana.api.{ Blob, Time }

import java.math.BigInteger
import java.nio.charset.Charset
import scala.collection.mutable.{ ArrayBuffer, ListBuffer }
import scala.reflect.ClassTag

class PostgresReaderWriter(charset: Charset) extends ByteReaderWriter[ByteBuf] {
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

  override def writeBytes(b: ByteBuf, v: ID[Array[Byte]]): TypedInt[Int] = {
    b.writeBytes(v)
    v.length
  }

  override def writeBytes(b: ByteBuf, offset: Int, v: ID[Array[Byte]]): TypedInt[Int] = {
    b.setBytes(offset, v)
    v.length
  }

  override def readInt(b: ByteBuf): Int = {
    b.readInt()
  }

  override def readInt(b: ByteBuf, offset: Int): Int = {
    b.getInt(offset)
  }

  override def writeInt(b: ByteBuf, v: Int): Int = {
    b.writeInt(4)
    b.writeInt(v)
    4
  }

  override def writeInt(b: ByteBuf, offset: Int, v: Int): Int = {
    b.writeInt(4)
    b.setInt(offset, v)
    4
  }
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
    tWrite(bb, v._1) + uWrite(bb, v._2)
  }

  override def readString(b: ByteBuf): String = {
    val length = b.readInt()
    val bytes = Array.ofDim[Byte](length)
    b.readBytes(bytes)
    new String(bytes, charset)
  }

  override def readString(b: ByteBuf, offset: Int): String = {
    val length = b.readInt()
    val bytes = Array.ofDim[Byte](length)
    b.getBytes(offset, bytes)
    new String(bytes, charset)
  }

  override def writeString(b: ByteBuf, v: String): Int = {
    val a = v.getBytes(charset)
    b.writeInt(a.length).writeBytes(a)
    a.length + 4
  }

  override def writeString(b: ByteBuf, offset: Int, v: String): Int = {
    val a = v.getBytes(charset)
    b.setInt(offset, a.length)
    b.setBytes(offset + 4, a)
    a.length + 4
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
      len
    }
  }

  def vLongSize(v: Long): Int = {
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

      bb.setByte(offset + 1, len.toByte)

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
        bb.setByte(offset + i + 2, ((ll & mask) >> shift).toByte)
        idx -= 1
        i += i
      }
      len
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

  override def readBigDecimal(b: ByteBuf): BigDecimal = {
    val scale = readVInt(b)
    val size = readVInt(b)
    val bytes = Array.ofDim[Byte](size)
    b.readBytes(bytes)
    new java.math.BigDecimal(new BigInteger(bytes), scale)
  }

  override def readBigDecimal(b: ByteBuf, offset: Int): BigDecimal = {
    val scale = readVInt(b, offset)
    val sScale = vLongSize(scale)
    val size = readVInt(b, offset + sScale)
    val sSize = vLongSize(size)
    val bytes = Array.ofDim[Byte](size)
    b.getBytes(offset + sScale + sSize, bytes)
    new java.math.BigDecimal(new BigInteger(bytes), scale)
  }

  override def writeBigDecimal(b: ByteBuf, v: BigDecimal): Int = {
    PostgresReaderWriter.writeNumericBinary(b, v.underlying())
  }

  override def writeBigDecimal(b: ByteBuf, offset: Int, v: BigDecimal): Int = {
    val u = v.underlying()
    val a = u.unscaledValue().toByteArray
    val s1 = writeVLong(b, offset, u.scale())
    val s2 = writeVLong(b, offset + s1, a.length)
    b.setBytes(offset + s1 + s2, a)
    s1 + s2 + a.length
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
    val p = b.readerIndex()
    b.readerIndex(offset)
    val size = readVInt(b)
    val result = ListBuffer.empty[T]

    for (_ <- 0 until size) {
      result += reader(b)
    }
    b.readerIndex(p)
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
    val p = b.writerIndex()
    b.writerIndex(offset)
    val s1 = writeVInt(b, offset, seq.size)
    val s = seq.foldLeft(s1)((s, v) => s + writer(b, v))
    b.writerIndex(p)
    s
  }

  override def readBlob(b: ByteBuf): Blob = {
    val size = readVInt(b)
    val data = new Array[Byte](size)
    b.readBytes(data)
    Blob(data)
  }

  override def readBlob(b: ByteBuf, offset: Int): Blob = {
    val p = b.readerIndex()
    b.readerIndex(offset)
    val size = readVInt(b)
    val data = new Array[Byte](size)
    b.readBytes(data)
    b.readerIndex(p)
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
}

object PostgresReaderWriter {
  private val POWERS10 = Array(1, 10, 100, 1000, 10000)
  private val MAX_GROUP_SCALE = 4
  private val MAX_GROUP_SIZE = POWERS10(4)
  private val NUMERIC_POSITIVE = 0x0000
  private val NUMERIC_NEGATIVE = 0x4000
//  private val NUMERIC_NAN = 0xC000.toShort
//  private val NUMERIC_CHUNK_MULTIPLIER = BigInteger.valueOf(10_000L)

  private def divide(unscaled: Array[BigInteger], divisor: Int) = {
    val bi = unscaled(0).divideAndRemainder(BigInteger.valueOf(divisor))
    unscaled(0) = bi(0)
    bi(1).intValue
  }

  // https://www.npgsql.org/dev/types.html
  // https://github.com/npgsql/npgsql/blob/8a479081f707784b5040747b23102c3d6371b9d3/
  //         src/Npgsql/TypeHandlers/NumericHandlers/NumericHandler.cs#L166
  private def writeNumericBinary(b: ByteBuf, value: java.math.BigDecimal): Int = {
    var weight = 0
    val groups = ArrayBuffer.empty[Int]
    var scale = value.scale
    val signum = value.signum
    if (signum != 0) {
      val unscaled: Array[BigInteger] = Array(null)
      if (scale < 0) {
        unscaled(0) = value.setScale(0).unscaledValue
        scale = 0
      } else unscaled(0) = value.unscaledValue
      if (signum < 0) unscaled(0) = unscaled(0).negate
      weight = -scale / MAX_GROUP_SCALE - 1
      var remainder = 0
      val scaleChunk = scale % MAX_GROUP_SCALE
      if (scaleChunk > 0) {
        remainder = divide(unscaled, POWERS10(scaleChunk)) * POWERS10(MAX_GROUP_SCALE - scaleChunk)
        if (remainder != 0) weight -= 1
      }
      if (remainder == 0) {
        remainder = divide(unscaled, MAX_GROUP_SIZE)
        while (remainder == 0) {
          remainder = divide(unscaled, MAX_GROUP_SIZE)
          weight += 1
        }
      }
      groups.append(remainder)
      while (unscaled(0).signum != 0) groups.append(divide(unscaled, MAX_GROUP_SIZE))
    }
    val groupCount = groups.size
    if (groupCount + weight > Short.MaxValue || scale > Short.MaxValue)
      throw new IllegalArgumentException(s"Invalid number value ${value.toString}")
    b.writeInt(8 + groupCount * 2)
    b.writeShort(groupCount)
    b.writeShort(groupCount + weight)
    b.writeShort(
      if (signum < 0) NUMERIC_NEGATIVE
      else NUMERIC_POSITIVE
    )
    b.writeShort(scale)
    for (i <- groupCount - 1 to 0 by -1) {
      b.writeShort(groups(i))
    }

    4 + 2 + 2 + 2 + 2 + groupCount * 2
  }
}
