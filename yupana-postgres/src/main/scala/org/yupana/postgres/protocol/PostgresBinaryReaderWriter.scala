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
import org.yupana.api.types.{ ByteReaderWriter, ID }
import org.yupana.api.{ Blob, Time }
import org.yupana.postgres.protocol.PostgresBinaryReaderWriter.{ PG_EPOCH_DIFF, readNumeric, writeNumeric }

import java.math.{ BigInteger, BigDecimal => JBigDecimal }
import java.nio.charset.Charset
import java.time.{ Duration, Instant, LocalDate, ZoneOffset }
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

class PostgresBinaryReaderWriter(charset: Charset) extends ByteReaderWriter[ByteBuf] {
  override def sizeOfBoolean: Int = ???

  override def sizeOfByte: Int = ???

  override def sizeOfInt: Int = ???

  override def sizeOfLong: Int = ???

  override def sizeOfDouble: Int = ???

  override def sizeOfShort: Int = ???

  override def sizeOfString(v: ID[String]): Int = ???

  override def sizeOfBigDecimal(v: ID[BigDecimal]): Int = ???

  override def sizeOfTime: Int = ???

  override def sizeOfTuple[T, U](v: (T, U), tSize: ID[T] => Int, uSize: ID[U] => Int): Int = ???

  override def sizeOfSeq[T](v: ID[Seq[T]], size: ID[T] => Int): Int = ???

  override def sizeOfBlob(v: ID[Blob]): Int = ???

  override def sizeOfPeriodDuration(v: ID[PeriodDuration]): Int = ???

  override def readBoolean(b: ByteBuf): Boolean = {
    checkSize(b, 1)
    b.readBoolean()
  }

  override def readBoolean(b: ByteBuf, offset: Int): Boolean = {
    checkSize(b, offset, 1)
    b.getBoolean(offset + 4)
  }

  override def writeBoolean(b: ByteBuf, v: Boolean): Int = {
    b.writeInt(1)
    b.writeBoolean(v)
    5
  }

  override def writeBoolean(b: ByteBuf, offset: Int, v: Boolean): Int = {
    b.setInt(offset, 1)
    b.setBoolean(offset + 4, v)
    5
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

  override def readInt(b: ByteBuf): Int = {
    checkSize(b, 4)
    b.readInt()
  }

  override def readInt(b: ByteBuf, offset: Int): Int = {
    checkSize(b, offset, 4)
    b.getInt(offset + 4)
  }

  override def writeInt(b: ByteBuf, v: Int): Int = {
    b.writeInt(4)
    b.writeInt(v)
    8
  }

  override def writeInt(b: ByteBuf, offset: Int, v: Int): Int = {
    b.setInt(offset, 4)
    b.setInt(offset + 4, v)
    8
  }
  override def readLong(b: ByteBuf): Long = {
    checkSize(b, 8)
    b.readLong()
  }

  override def readLong(b: ByteBuf, offset: Int): Long = {
    checkSize(b, offset, 8)
    b.getLong(offset + 4)
  }

  override def writeLong(b: ByteBuf, v: Long): Int = {
    b.writeInt(8)
    b.writeLong(v)
    12
  }

  override def writeLong(b: ByteBuf, offset: Int, v: Long): Int = {
    b.setInt(offset, 8)
    b.setLong(offset + 4, v)
    12
  }

  override def readDouble(b: ByteBuf): Double = {
    checkSize(b, 8)
    b.readDouble()
  }

  override def readDouble(b: ByteBuf, offset: Int): Double = {
    checkSize(b, offset, 8)
    b.getDouble(offset + 4)
  }

  override def writeDouble(b: ByteBuf, v: Double): Int = {
    b.writeInt(8)
    b.writeDouble(v)
    12
  }

  override def writeDouble(b: ByteBuf, offset: Int, v: Double): Int = {
    b.setInt(offset, 8)
    b.setDouble(offset + 4, v)
    12
  }

  override def readShort(b: ByteBuf): Short = {
    checkSize(b, 2)
    b.readShort()
  }

  override def readShort(b: ByteBuf, offset: Int): Short = {
    checkSize(b, offset, 2)
    b.getShort(offset + 4)
  }

  override def writeShort(b: ByteBuf, v: Short): Int = {
    b.writeInt(2)
    b.writeShort(v)
    6
  }

  override def writeShort(b: ByteBuf, offset: Int, v: Short): Int = {
    b.setInt(offset, 2)
    b.setShort(offset + 4, v)
    6
  }

  override def readByte(b: ByteBuf): Byte = {
    readNumeric(b).byteValue()
  }

  override def readByte(b: ByteBuf, offset: Int): Byte = {
    readNumeric(b, offset).byteValue()
  }

  override def writeByte(b: ByteBuf, v: Byte): Int = {
    writeNumeric(b, JBigDecimal.valueOf(v))
  }

  override def writeByte(b: ByteBuf, offset: Int, v: Byte): Int = {
    writeNumeric(b, offset, JBigDecimal.valueOf(v))
  }

  def doubleToTime(dbl: Double): Time = {
    val secs = dbl + PG_EPOCH_DIFF
    val millis = math.round((secs % 1) * 1_000)
    Time(secs.toLong * 1000 + millis)
  }

  def timeToDouble(v: Time): Double = {
    val sec = v.millis / 1000
    val nanos = (v.millis % 1000).toDouble * 1_000_000
    (sec - PostgresBinaryReaderWriter.PG_EPOCH_DIFF).toDouble + nanos / 1_000_000_000
  }

  override def readTime(b: ByteBuf): Time = {
    val dbl = java.lang.Double.longBitsToDouble(readLong(b))
    doubleToTime(dbl)
  }

  override def readTime(b: ByteBuf, offset: Int): Time = {
    val dbl = java.lang.Double.longBitsToDouble(readLong(b, offset))
    doubleToTime(dbl)
  }

  override def writeTime(b: ByteBuf, v: Time): Int = {
    val value = timeToDouble(v)
    writeLong(b, java.lang.Double.doubleToLongBits(value))
  }

  override def writeTime(b: ByteBuf, offset: Int, v: Time): Int = {
    val value = timeToDouble(v)
    writeLong(b, offset, java.lang.Double.doubleToLongBits(value))
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
    val length = b.getInt(offset)
    val bytes = Array.ofDim[Byte](length)
    b.getBytes(offset + 4, bytes)
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
    readNumeric(b)
  }

  override def readBigDecimal(b: ByteBuf, offset: Int): BigDecimal = {
    readNumeric(b, offset)
  }

  override def writeBigDecimal(b: ByteBuf, v: BigDecimal): Int = {
    writeNumeric(b, v.underlying())
  }

  override def writeBigDecimal(b: ByteBuf, offset: Int, v: BigDecimal): Int = {
    writeNumeric(b, offset, v.underlying())
  }

  override def readSeq[T: ClassTag](b: ByteBuf, reader: ByteBuf => T): Seq[T] = {
    val size = b.readInt()
    val result = ListBuffer.empty[T]

    for (_ <- 0 until size) {
      result += reader(b)
    }

    result.toSeq
  }

  override def readSeq[T: ClassTag](b: ByteBuf, offset: Int, reader: ByteBuf => T): Seq[T] = {
    val slice = b.slice(offset, b.capacity() - offset)
    val size = slice.readInt()
    val result = ListBuffer.empty[T]

    for (_ <- 0 until size) {
      result += reader(slice)
    }
    result.toSeq
  }

  override def writeSeq[T](b: ByteBuf, seq: Seq[T], writer: (ByteBuf, T) => Int)(
      implicit ct: ClassTag[T]
  ): Int = {
    b.writeInt(seq.size)
    seq.foldLeft(4)((s, v) => s + writer(b, v))
  }

  override def writeSeq[T](b: ByteBuf, offset: Int, seq: Seq[T], writer: (ByteBuf, T) => Int)(
      implicit ct: ClassTag[T]
  ): Int = {
    val slice = b.slice(offset, b.capacity() - offset)
    slice.writerIndex(0)
    slice.writeInt(seq.size)
    seq.foldLeft(4)((s, v) => s + writer(slice, v))
  }

  override def readBlob(b: ByteBuf): Blob = {
    val size = b.readInt()
    val data = new Array[Byte](size)
    b.readBytes(data)
    Blob(data)
  }

  override def readBlob(b: ByteBuf, offset: Int): Blob = {
    val size = b.getInt(offset)
    val data = new Array[Byte](size)
    b.getBytes(offset + 4, data)
    Blob(data)
  }

  override def writeBlob(b: ByteBuf, v: Blob): Int = {
    b.writeInt(v.bytes.length)
    b.writeBytes(v.bytes)
    4 + v.bytes.length
  }

  override def writeBlob(b: ByteBuf, offset: Int, v: Blob): Int = {
    b.setInt(offset, v.bytes.length)
    b.setBytes(offset + 4, v.bytes)
    4 + v.bytes.length
  }

  override def readVTime(b: ByteBuf): Time = {
    readTime(b)
  }

  override def readVTime(b: ByteBuf, offset: Int): Time = {
    readTime(b, offset)
  }
  override def writeVTime(b: ByteBuf, v: Time): Int = {
    writeTime(b, v)
  }

  override def writeVTime(b: ByteBuf, offset: Int, v: Time): Int = {
    writeTime(b, offset, v)
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

  private def checkSize(b: ByteBuf, expected: Int): Unit = {
    val s = b.readInt()
    if (s != expected) throw new IllegalArgumentException(s"Expected size $expected, but got $s")
  }

  private def checkSize(b: ByteBuf, offset: Int, expected: Int): Unit = {
    val s = b.getInt(offset)
    if (s != expected) throw new IllegalArgumentException(s"Expected size $expected, but got $s")
  }
}

object PostgresBinaryReaderWriter {
  private val INT_TEN_POWERS = Array(1, 10, 100, 1000, 10000)
  private val BI_TEN_POWERS = Array.iterate(BigInteger.ONE, 32)(_.multiply(BigInteger.TEN))
  private val NUMERIC_POSITIVE = 0x0000
  private val NUMERIC_NEGATIVE = 0x4000
  private val NUMERIC_NAN = 0xC000.toShort
  private val BI_TEN_THOUSAND = BigInteger.valueOf(10_000L)
  private val BI_MAX_LONG = BigInteger.valueOf(Long.MaxValue)

  private val PG_EPOCH_DIFF =
    Duration.between(Instant.EPOCH, LocalDate.of(2000, 1, 1).atStartOfDay.toInstant(ZoneOffset.UTC)).toSeconds

  def writeNumeric(b: ByteBuf, value: JBigDecimal): Int = {
    var shorts = List.empty[Short]
    var unscaled = value.unscaledValue.abs
    var scale = value.scale

    if (unscaled == BigInteger.ZERO) {
      val bytes = Array[Byte](0, 0, -1, -1, 0, 0)
      b.writeInt(8)
      b.writeBytes(bytes)
      b.writeShort(math.max(0, scale))
      12
    } else {
      var weight = -1
      if (scale <= 0) {
        // this means we have an integer
        // adjust unscaled and weight
        if (scale < 0) {
          scale = Math.abs(scale)
          // weight value covers 4 digits
          weight += scale / 4
          // whatever remains needs to be incorporated to the unscaled value
          val mod = scale % 4
          unscaled = unscaled.multiply(tenPower(mod))
          scale = 0
        }
        while (unscaled.compareTo(BI_MAX_LONG) > 0) {
          val pair = unscaled.divideAndRemainder(BI_TEN_THOUSAND)
          unscaled = pair(0)
          val shortValue = pair(1).shortValue
          if (shortValue != 0 || shorts.nonEmpty) shorts ::= shortValue
          weight += 1
        }
        var unscaledLong = unscaled.longValueExact
        do {
          val shortValue = (unscaledLong % 10000).toShort
          if (shortValue != 0 || shorts.nonEmpty) shorts ::= shortValue
          unscaledLong = unscaledLong / 10000L
          weight += 1
        } while (unscaledLong != 0)
      } else {
        val split = unscaled.divideAndRemainder(tenPower(scale))
        var decimal = split(1)
        var wholes = split(0)
        weight = -1
        if (BigInteger.ZERO != decimal) {
          val mod = scale % 4
          var segments = scale / 4
          if (mod != 0) {
            decimal = decimal.multiply(tenPower(4 - mod))
            segments += 1
          }
          do {
            val pair = decimal.divideAndRemainder(BI_TEN_THOUSAND)
            decimal = pair(0)
            val shortValue = pair(1).shortValue
            if (shortValue != 0 || shorts.nonEmpty) shorts ::= shortValue
            segments -= 1
          } while (BigInteger.ZERO != decimal)
          // for the leading 0 shorts we either adjust weight (if no wholes)
          // or push shorts
          if (BigInteger.ZERO == wholes) {
            weight -= segments
          } else {
            // now add leading 0 shorts
            for (_ <- 0 until segments) {
              shorts ::= 0.toShort
            }
          }
        }
        while (BigInteger.ZERO != wholes) {
          weight += 1
          val pair = wholes.divideAndRemainder(BI_TEN_THOUSAND)
          wholes = pair(0)
          val shortValue = pair(1).shortValue
          if (shortValue != 0 || shorts.nonEmpty) shorts ::= shortValue
        }
      }

      val groupCount = shorts.length

      b.writeInt(8 + groupCount * 2)

      // 8 bytes for "header" and then 2 for each short//8 bytes for "header" and then 2 for each short
      // number of 2-byte shorts representing 4 decimal digits//number of 2-byte shorts representing 4 decimal digits
      b.writeShort(groupCount)
      // 0 based number of 4 decimal digits (i.e. 2-byte shorts) before the decimal//0 based number of 4 decimal digits (i.e. 2-byte shorts) before the decimal
      b.writeShort(weight)
      // indicates positive, negative or NaN//indicates positive, negative or NaN
      b.writeShort(if (value.signum() == -1) NUMERIC_NEGATIVE else NUMERIC_POSITIVE)
      // number of digits after the decimal//number of digits after the decimal
      b.writeShort(math.max(0, scale))

      shorts.foreach(x => b.writeShort(x))
      4 + 2 + 2 + 2 + 2 + groupCount * 2
    }
  }

  private def writeNumeric(b: ByteBuf, offset: Int, value: JBigDecimal): Int = {
    val slice = b.slice(offset, b.capacity() - offset)
    slice.writerIndex(0)
    writeNumeric(slice, value)
  }

  private def tenPower(exponent: Int): BigInteger = {
    if (BI_TEN_POWERS.length > exponent) BI_TEN_POWERS(exponent) else BigInteger.TEN.pow(exponent)
  }

  // Implementation is based on Postgresql JDBC driver:
  // https://github.com/pgjdbc/pgjdbc/blob/9a389df3067b8e880a1b4149845eed478d3a82a0/pgjdbc/src/main/java/org/postgresql/util/ByteConverter.java#L124
  private def readNumeric(b: ByteBuf): JBigDecimal = {
    val size = b.readInt()
    if (size < 8) throw new IllegalArgumentException("number of bytes should be at-least 8")

    val len = b.readShort()
    var weight = b.readShort().toInt
    val sign = b.readShort()
    val scale = b.readShort().toInt

    if (len * 2 + 8 != size) throw new IllegalArgumentException(s"DECIMAL size is incorrect, ${len * 2 + 8} != $size")
    if (sign == NUMERIC_NAN) throw new IllegalArgumentException("DECIMAL cannot be NaN")
    if (sign != NUMERIC_POSITIVE && sign != NUMERIC_NEGATIVE)
      throw new IllegalArgumentException(s"Invalid sign, $sign")

    if ((scale & 0x3FFF) != scale) throw new IllegalArgumentException(s"Invalid scale, $scale")
    if (len == 0) {
      new JBigDecimal(BigInteger.ZERO, scale)
    } else {
      var d = b.readShort().toInt
      if (weight < 0) {
        assert(scale > 0)
        var effectiveScale = scale
        weight += 1
        if (weight < 0) effectiveScale += 4 * weight
        var i = 1
        while (i < len && d == 0) {
          effectiveScale -= 4
          d = b.readShort()
          i += 1
        }
        assert(effectiveScale > 0)
        if (effectiveScale >= 4) effectiveScale -= 4
        else {
          d = (d / INT_TEN_POWERS(4 - effectiveScale)).toShort
          effectiveScale = 0
        }
        var unscaledBI: BigInteger = null
        var unscaledInt = d.toLong

        while (i < len) {
          if (i == 4 && effectiveScale > 2) unscaledBI = BigInteger.valueOf(unscaledInt)
          d = b.readShort()
          if (effectiveScale >= 4) {
            if (unscaledBI == null) unscaledInt *= 10000
            else unscaledBI = unscaledBI.multiply(BI_TEN_THOUSAND)
            effectiveScale -= 4
          } else {
            if (unscaledBI == null) unscaledInt *= INT_TEN_POWERS(effectiveScale)
            else unscaledBI = unscaledBI.multiply(tenPower(effectiveScale))
            d = (d / INT_TEN_POWERS(4 - effectiveScale)).toShort
            effectiveScale = 0
          }
          if (unscaledBI == null) unscaledInt += d
          else if (d != 0) unscaledBI = unscaledBI.add(BigInteger.valueOf(d))

          i += 1
        }
        if (unscaledBI == null) unscaledBI = BigInteger.valueOf(unscaledInt)
        if (effectiveScale > 0) unscaledBI = unscaledBI.multiply(tenPower(effectiveScale))
        if (sign == NUMERIC_NEGATIVE) unscaledBI = unscaledBI.negate
        new JBigDecimal(unscaledBI, scale)
      } else if (scale == 0) {
        var unscaledBI: BigInteger = null
        var unscaledInt = d.toLong
        for (i <- 1 until len) {
          if (i == 4) unscaledBI = BigInteger.valueOf(unscaledInt)
          d = b.readShort()
          if (unscaledBI == null) {
            unscaledInt *= 10000
            unscaledInt += d
          } else {
            unscaledBI = unscaledBI.multiply(BI_TEN_THOUSAND)
            if (d != 0) unscaledBI = unscaledBI.add(BigInteger.valueOf(d))
          }
        }
        if (unscaledBI == null) unscaledBI = BigInteger.valueOf(unscaledInt)
        if (sign == NUMERIC_NEGATIVE) unscaledBI = unscaledBI.negate
        val bigDecScale = (len - (weight + 1)) * 4
        if (bigDecScale == 0) new JBigDecimal(unscaledBI)
        else new JBigDecimal(unscaledBI, bigDecScale).setScale(0)
      } else {
        var unscaledBI: BigInteger = null
        var unscaledInt = d.toLong
        var effectiveWeight = weight
        var effectiveScale = scale
        for (i <- 1 until len) {
          if (i == 4) unscaledBI = BigInteger.valueOf(unscaledInt)
          d = b.readShort()
          if (effectiveWeight > 0) {
            effectiveWeight -= 1
            if (unscaledBI == null) unscaledInt *= 10000
            else unscaledBI = unscaledBI.multiply(BI_TEN_THOUSAND)
          } else if (effectiveScale >= 4) {
            effectiveScale -= 4
            if (unscaledBI == null) unscaledInt *= 10000
            else unscaledBI = unscaledBI.multiply(BI_TEN_THOUSAND)
          } else {
            if (unscaledBI == null) unscaledInt *= INT_TEN_POWERS(effectiveScale)
            else unscaledBI = unscaledBI.multiply(tenPower(effectiveScale))
            d = (d / INT_TEN_POWERS(4 - effectiveScale)).toShort
            effectiveScale = 0
          }
          if (unscaledBI == null) unscaledInt += d
          else if (d != 0) unscaledBI = unscaledBI.add(BigInteger.valueOf(d))
        }
        if (unscaledBI == null) unscaledBI = BigInteger.valueOf(unscaledInt)
        if (effectiveWeight > 0) unscaledBI = unscaledBI.multiply(tenPower(effectiveWeight * 4))
        if (effectiveScale > 0) unscaledBI = unscaledBI.multiply(tenPower(effectiveScale))
        if (sign == NUMERIC_NEGATIVE) unscaledBI = unscaledBI.negate
        new JBigDecimal(unscaledBI, scale)
      }
    }
  }

  private def readNumeric(b: ByteBuf, offset: Int): JBigDecimal = {
    readNumeric(b.slice(offset, b.capacity() - offset))
  }
}
