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

import java.math.{ BigInteger, BigDecimal => JBigDecimal }
import java.nio.charset.Charset
import scala.collection.mutable.{ ArrayBuffer, ListBuffer }
import scala.reflect.ClassTag

class PostgresBinaryReaderWriter(charset: Charset) extends ByteReaderWriter[ByteBuf] {
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
    assert(b.readInt() == 4)
    b.readInt()
  }

  override def readInt(b: ByteBuf, offset: Int): Int = {
    assert(b.getInt(offset) == 4)
    b.getInt(offset + 4)
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
    b.writeInt(8)
    val sec = v.millis / 1000
    val nanos = (v.millis - sec * 1000).toDouble / 1000
    val long = java.lang.Double.doubleToLongBits(sec)
    b.writeLong(long)
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
    readLong(bb, offset)
  }

  override def readVLong(bb: ByteBuf): Long = {
    readLong(bb)
  }

  override def writeVLong(bb: ByteBuf, v: Long): Int = {
    writeLong(bb, v)
  }

  override def writeVLong(bb: ByteBuf, offset: Int, v: Long): Int = {
    writeLong(bb, offset, v)
  }

  override def readVInt(b: ByteBuf): Int = {
    readInt(b)
  }

  override def readVInt(b: ByteBuf, offset: Int): Int = {
    readInt(b, offset)
  }

  override def writeVInt(b: ByteBuf, v: Int): Int = {
    writeInt(b, v)
  }

  override def writeVInt(b: ByteBuf, offset: Int, v: Int): Int = {
    writeInt(b, offset, v)
  }

  override def readVShort(b: ByteBuf): Short = {
    readShort(b)
  }

  override def readVShort(b: ByteBuf, offset: Int): Short = {
    readShort(b, offset)
  }

  override def writeVShort(b: ByteBuf, v: Short): Int = {
    writeShort(b, v)
  }
  override def writeVShort(b: ByteBuf, offset: Int, v: Short): Int = {
    writeShort(b, offset, v)
  }

  override def readBigDecimal(b: ByteBuf): BigDecimal = {
    PostgresBinaryReaderWriter.readNumeric(b)
  }

  override def readBigDecimal(b: ByteBuf, offset: Int): BigDecimal = {
    ???
  }

  override def writeBigDecimal(b: ByteBuf, v: BigDecimal): Int = {
    PostgresBinaryReaderWriter.writeNumeric(b, v.underlying())
  }

  override def writeBigDecimal(b: ByteBuf, offset: Int, v: BigDecimal): Int = {
    ???
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

object PostgresBinaryReaderWriter {
  private val POWERS10 = Array(1, 10, 100, 1000, 10000)
  private val MAX_GROUP_SCALE = 4
  private val MAX_GROUP_SIZE = POWERS10(4)
  private val NUMERIC_POSITIVE = 0x0000
  private val NUMERIC_NEGATIVE = 0x4000
  private val NUMERIC_NAN = 0xC000.toShort
  private val NUMERIC_CHUNK_MULTIPLIER = BigInteger.valueOf(10_000L)

  private def divide(unscaled: Array[BigInteger], divisor: Int) = {
    val bi = unscaled(0).divideAndRemainder(BigInteger.valueOf(divisor))
    unscaled(0) = bi(0)
    bi(1).intValue
  }

  // This implementation is based on the H2 server:
  // https://github.com/h2database/h2database/blob/master/h2/src/main/org/h2/server/pg/PgServerThread.java#L761
  private def writeNumeric(b: ByteBuf, value: JBigDecimal): Int = {
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

  private def readNumeric(b: ByteBuf): JBigDecimal = {
    val size = b.readInt()
    if (size < 8) throw new IllegalArgumentException(s"DECIMAL size is to small, $size")

    val len = b.readShort()
    val weight = b.readShort()
    val sign = b.readShort()
    val scale = b.readShort()
    if (len * 2 + 8 != size) throw new IllegalArgumentException(s"DECIMAL size is incorrect, ${len * 2 + 8} != $size")
    if (sign == NUMERIC_NAN) throw new IllegalArgumentException("DECIMAL cannot be NaN")
    if (sign != NUMERIC_POSITIVE && sign != NUMERIC_NEGATIVE)
      throw new IllegalArgumentException(s"Invalid sign, $sign")
    if ((scale & 0x3FFF) != scale) throw throw new IllegalArgumentException(s"Invalid scale, $scale")
    if (len == 0) {
      if (scale == 0) JBigDecimal.ZERO
      else new JBigDecimal(BigInteger.ZERO, scale)
    } else {
      var n = BigInteger.ZERO
      for (i <- 0 until len) {
        val c = b.readShort()
        if (c < 0 || c > 9_999) throw new IllegalArgumentException(s"Incorrect chunk $c")
        n = n.multiply(NUMERIC_CHUNK_MULTIPLIER).add(BigInteger.valueOf(c))
      }
      if (sign != NUMERIC_POSITIVE) n = n.negate
      new JBigDecimal(n, (len - weight - 1) * 4).setScale(scale)
    }
  }
}
