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

import java.math.{ BigDecimal => JavaBigDecimal }
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import org.joda.time.Period
import org.joda.time.format.{ ISOPeriodFormat, PeriodFormatter }
import org.yupana.api.Time

import scala.annotation.implicitNotFound

/**
  * Byte array serialization type class
  * @tparam T type to be serialized
  */
@implicitNotFound("No member of type class Writable for class ${T} is found")
trait Writable[T] extends Serializable {

  /**
    * Serialize instance of T into array of bytes
    * @param t value to be serialized
    * @return serialized bytes
    */
  def write(t: T): Array[Byte]
}

object Writable {
  private val periodFormat: PeriodFormatter = ISOPeriodFormat.standard()

  implicit val booleanWritable: Writable[Boolean] = of(x => Array[Byte](if (x) 1 else 0))
  implicit val doubleWritable: Writable[Double] = of(d => ByteBuffer.allocate(8).putDouble(d).array())
  implicit val intWritable: Writable[Int] = of(i => vLongToBytes(i))
  implicit val bigDecimalWritable: Writable[BigDecimal] = of(x => bigDecimalToBytes(x.underlying()))
  implicit val longWritable: Writable[Long] = of(vLongToBytes)
  implicit val stringWritable: Writable[String] = of(stringToBytes)
  implicit val timeWritable: Writable[Time] = of(t => longWritable.write(t.millis))
  implicit val periodWritable: Writable[Period] = of(p => stringWritable.write(periodFormat.print(p)))

  implicit def arrayWritable[T](implicit wt: Writable[T]): Writable[Array[T]] = of(arrayToBytes(wt))

  def of[T](f: T => Array[Byte]): Writable[T] = new Writable[T] {
    override def write(t: T): Array[Byte] = f(t)
  }

  def noop[T]: Writable[T] = new Writable[T] {
    override def write(t: T): Array[Byte] = throw new IllegalStateException("This should not be written")
  }

  private def bigDecimalToBytes(x: JavaBigDecimal): Array[Byte] = {
    val a = x.unscaledValue().toByteArray
    val scale = vLongToBytes(x.scale())
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

  private def arrayToBytes[T](writable: Writable[T])(array: Array[T]): Array[Byte] = {
    val bytes = array.map(writable.write)
    val arraySize = vLongToBytes(array.length)
    val resultSize = bytes.foldLeft(arraySize.length)((a, i) => a + i.length)
    val bb = ByteBuffer.allocate(resultSize)
    bb.put(arraySize)
    bytes.foreach(bb.put)
    bb.array()
  }
}
