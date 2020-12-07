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

import java.nio.ByteBuffer

import org.joda.time.Period
import org.joda.time.format.{ ISOPeriodFormat, PeriodFormatter }
import org.yupana.api.{ Blob, Serialization, Time }

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
  private val periodFormat: PeriodFormatter = ISOPeriodFormat.standard()

  implicit val booleanStorable: Storable[Boolean] = of(Serialization.readBoolean, Serialization.writeBoolean)
  implicit val doubleStorable: Storable[Double] = of(Serialization.readDouble, Serialization.writeDouble)
  implicit val bigDecimalStorable: Storable[BigDecimal] =
    of(Serialization.readBigDecimal, x => Serialization.writeBigDecimal(x.underlying()))
  implicit val byteStorable: Storable[Byte] = of(Serialization.readByte, Serialization.writeByte)
  implicit val shortStorable: Storable[Short] = of(Serialization.readVShort, Serialization.writeVShort)
  implicit val intStorable: Storable[Int] = of(Serialization.readVInt, Serialization.writeVInt)
  implicit val longStorable: Storable[Long] = of(Serialization.readVLong, Serialization.writeVLong)
  implicit val stringStorable: Storable[String] = of(Serialization.readString, Serialization.writeString)
  implicit val timestampStorable: Storable[Time] = wrap(longStorable, (l: Long) => new Time(l), _.millis)
  implicit val periodStorable: Storable[Period] =
    wrap(stringStorable, (s: String) => ISOPeriodFormat.standard().parsePeriod(s), p => periodFormat.print(p))

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

  private def readSeq[T: ClassTag](storable: Storable[T])(bb: ByteBuffer): Seq[T] = {
    val size = Serialization.readVInt(bb)
    val result = ListBuffer.empty[T]

    for (_ <- 0 until size) {
      result += storable.read(bb)
    }

    result
  }

  private def seqToBytes[T](storable: Storable[T])(array: Seq[T]): Array[Byte] = {
    val bytes = array.map(storable.write)
    val arraySize = Serialization.writeVLong(array.length)
    val resultSize = bytes.foldLeft(arraySize.length)((a, i) => a + i.length)
    val bb = ByteBuffer.allocate(resultSize)
    bb.put(arraySize)
    bytes.foreach(bb.put)
    bb.array()
  }

  private def readBlob(bb: ByteBuffer): Blob = {
    Blob(Serialization.readBytes(bb))
  }

  private def blobToBytes(blob: Blob): Array[Byte] = {
    Serialization.writeBytes(blob.bytes)
  }
}
