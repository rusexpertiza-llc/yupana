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

package org.yupana.api.types

import org.threeten.extra.PeriodDuration

import java.math.{ BigInteger, BigDecimal => JavaBigDecimal }
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import org.yupana.api.{ Blob, Time }

import scala.annotation.implicitNotFound
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
  * Byte array serialization/deserialization type class
  * @tparam T type to be serialized/deserialized
  */
@implicitNotFound("No member of type class Storable for class ${T} is found")
trait Storable[T] extends Serializable {

  /**
    * Reads an object from array of bytes
    * @param a bytes to be read
    * @return deserialized object
    */
  def read(a: Array[Byte]): T

  /**
    * Reads an object from byte buffer
    * @param b byte buffer to read bytes
    * @return deserialized object
    */
  def read(b: ByteBuffer): T

  /**
    * Serialize instance of T into array of bytes
    * @param t value to be serialized
    * @return serialized bytes
    */
  def write(t: T): Array[Byte]
}

object Storable {
  implicit val booleanStorable: Storable[Boolean] = of(_.get() != 0, x => Array[Byte](if (x) 1 else 0))
  implicit val doubleStorable: Storable[Double] = of(_.getDouble, d => ByteBuffer.allocate(8).putDouble(d).array())
  implicit val bigDecimalStorable: Storable[BigDecimal] = of(readBigDecimal, bigDecimalToBytes)
  implicit val byteStorable: Storable[Byte] = of(_.get, Array(_))
  implicit val shortStorable: Storable[Short] = of(readVShort, s => vLongToBytes(s))
  implicit val intStorable: Storable[Int] = of(readVInt, i => vLongToBytes(i))
  implicit val longStorable: Storable[Long] = of(readVLong, vLongToBytes)
  implicit val stringStorable: Storable[String] = of(readString, stringToBytes)
  implicit val timestampStorable: Storable[Time] = wrap(longStorable, (l: Long) => new Time(l), _.millis)
  implicit val periodStorable: Storable[PeriodDuration] =
    wrap(stringStorable, PeriodDuration.parse, _.toString)

  implicit val blobStorable: Storable[Blob] = of(readBlob, blobToBytes)

  implicit def seqStorable[T](implicit rt: Storable[T], ct: ClassTag[T]): Storable[Seq[T]] =
    of(readSeq(rt), seqToBytes(rt))

  def of[T](r: ByteBuffer => T, w: T => Array[Byte]): Storable[T] = new Storable[T] {
    override def read(a: Array[Byte]): T = r(ByteBuffer.wrap(a))
    override def read(b: ByteBuffer): T = r(b)
    override def write(t: T): Array[Byte] = w(t)
  }

  def noop[T]: Storable[T] = new Storable[T] {
    override def read(a: Array[Byte]): T = throw new IllegalStateException("This should not be read")
    override def read(b: ByteBuffer): T = throw new IllegalStateException("This should not be read")
    override def write(t: T): Array[Byte] = throw new IllegalStateException("This should not be written")
  }

  def wrap[T, U](storable: Storable[T], from: T => U, to: U => T): Storable[U] = new Storable[U] {
    override def read(a: Array[Byte]): U = from(storable.read(a))
    override def read(bb: ByteBuffer): U = from(storable.read(bb))
    override def write(t: U): Array[Byte] = storable.write(to(t))
  }

  private def readBigDecimal(bb: ByteBuffer): JavaBigDecimal = {
    val scale = readVInt(bb)
    val size = readVInt(bb)
    val bytes = Array.ofDim[Byte](size)
    bb.get(bytes)
    new JavaBigDecimal(new BigInteger(bytes), scale)
  }

  private def readString(bb: ByteBuffer): String = {
    val length = bb.getInt()
    val bytes = Array.ofDim[Byte](length)
    bb.get(bytes)
    new String(bytes, StandardCharsets.UTF_8)
  }

  private def readVShort(bb: ByteBuffer): Short = {
    val l = readVLong(bb)
    if (l <= Short.MaxValue && l >= Short.MinValue) l.toShort
    else throw new IllegalArgumentException("Got Long but Short expected")
  }

  private def readVInt(bb: ByteBuffer): Int = {
    val l = readVLong(bb)
    if (l <= Int.MaxValue && l >= Int.MinValue) l.toInt
    else throw new IllegalArgumentException("Got Long but Int expected")
  }

  private def readVLong(bb: ByteBuffer): Long = {
    val first = bb.get()

    val len = if (first >= -112) {
      1
    } else if (first >= -120) {
      -111 - first
    } else {
      -119 - first
    }

    var result = 0L

    if (len == 1) {
      first
    } else {

      0 until (len - 1) foreach { _ =>
        val b = bb.get()
        result <<= 8
        result |= (b & 0xFF)
      }

      if (first >= -120) result else result ^ -1L
    }
  }

  private def readSeq[T: ClassTag](storable: Storable[T])(bb: ByteBuffer): Seq[T] = {
    val size = readVInt(bb)
    val result = ListBuffer.empty[T]

    for (_ <- 0 until size) {
      result += storable.read(bb)
    }

    result.toSeq
  }

  private def bigDecimalToBytes(x: BigDecimal): Array[Byte] = {
    val u = x.underlying()
    val a = u.unscaledValue().toByteArray
    val scale = vLongToBytes(u.scale())
    val length = vLongToBytes(a.length)
    ByteBuffer
      .allocate(a.length + scale.length + length.length)
      .put(scale)
      .put(length)
      .put(a)
      .array()
  }

  private def stringToBytes(s: String): Array[Byte] = {
    val a = s.getBytes(StandardCharsets.UTF_8)
    ByteBuffer
      .allocate(a.length + 4)
      .putInt(a.length)
      .put(a)
      .array()
  }

  private def vLongToBytes(l: Long): Array[Byte] = {
    if (l <= 127 && l > -112) {
      Array(l.toByte)
    } else {
      var ll = l
      val bb = ByteBuffer.allocate(9)
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

      (len - 1 to 0 by -1) foreach { idx =>
        val shift = idx * 8
        val mask = 0xFFL << shift
        bb.put(((ll & mask) >> shift).toByte)
      }

      val res = new Array[Byte](bb.position())
      bb.rewind()
      bb.get(res)
      res
    }
  }

  private def seqToBytes[T](storable: Storable[T])(array: Seq[T]): Array[Byte] = {
    val bytes = array.map(storable.write)
    val arraySize = vLongToBytes(array.length)
    val resultSize = bytes.foldLeft(arraySize.length)((a, i) => a + i.length)
    val bb = ByteBuffer.allocate(resultSize)
    bb.put(arraySize)
    bytes.foreach(bb.put)
    bb.array()
  }

  private def readBlob(bb: ByteBuffer): Blob = {
    val size = readVInt(bb)
    val data = new Array[Byte](size)
    bb.get(data)
    Blob(data)
  }

  private def blobToBytes(blob: Blob): Array[Byte] = {
    val sizeBytes = vLongToBytes(blob.bytes.length)
    sizeBytes ++ blob.bytes
  }
}
