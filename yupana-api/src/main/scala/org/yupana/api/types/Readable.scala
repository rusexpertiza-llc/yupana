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

import java.math.{ BigInteger, BigDecimal => JavaBigDecimal }
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import org.joda.time.Period
import org.joda.time.format.ISOPeriodFormat
import org.yupana.api.Time

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag

/**
  * Byte array deserialization type class
  * @tparam T type to be deserialized
  */
@implicitNotFound("No member of type class Readable for class ${T} is found")
trait Readable[T] extends Serializable {

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
}

object Readable {
  implicit val booleanReadable: Readable[Boolean] = of(_.get() != 0)
  implicit val doubleReadable: Readable[Double] = of(_.getDouble)
  implicit val intReadable: Readable[Int] = of(readVInt)
  implicit val bigDecimalReadable: Readable[BigDecimal] = of(readBigDecimal)
  implicit val longReadable: Readable[Long] = of(readVLong)
  implicit val stringReadable: Readable[String] = of(readString)
  implicit val timestampReadable: Readable[Time] = of(bb => Time(longReadable.read(bb)))
  implicit val periodReadable: Readable[Period] = of(
    bb => ISOPeriodFormat.standard().parsePeriod(stringReadable.read(bb))
  )

  implicit def arrayReadable[T](implicit rt: Readable[T], ct: ClassTag[T]): Readable[Array[T]] = of(readArray(rt))

  def of[T](f: ByteBuffer => T): Readable[T] = new Readable[T] {
    override def read(a: Array[Byte]): T = f(ByteBuffer.wrap(a))
    override def read(b: ByteBuffer): T = f(b)
  }

  def noop[T]: Readable[T] = new Readable[T] {
    override def read(a: Array[Byte]): T = throw new IllegalStateException("This should not be read")
    override def read(b: ByteBuffer): T = throw new IllegalStateException("This should not be read")
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
        result |= (b & 0xff)
      }

      if (first >= -120) result else result ^ -1L
    }
  }

  private def readArray[T: ClassTag](readable: Readable[T])(bb: ByteBuffer): Array[T] = {
    val size = readVInt(bb)
    val result = new Array[T](size)

    for (i <- 0 until size) {
      result(i) = readable.read(bb)
    }

    result
  }
}
