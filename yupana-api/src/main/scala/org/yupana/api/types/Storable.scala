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
import org.yupana.api.{ Blob, Currency, Time }

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag

/**
  * Byte array serialization/deserialization type class
  * @tparam T type to be serialized/deserialized
  */
@implicitNotFound("No member of type class Storable for class ${T} is found")
trait Storable[T] extends Serializable {

  /**
    * Reads an object from byte buffer
    * @param b byte buffer to read bytes
    * @return deserialized object
    */
  def read[B, V[_], S, O](b: B)(implicit rw: ReaderWriter[B, V, S, O]): V[T]

  def read[B, V[_], S, O](b: B, offset: O)(implicit rw: ReaderWriter[B, V, S, O]): V[T]

  /**
    * Serialize instance of T into array of bytes
    * @param t value to be serialized
    * @return serialized bytes
    */
  def write[B, V[_], S, O](b: B, v: V[T])(implicit rw: ReaderWriter[B, V, S, O]): S

  def write[B, V[_], S, O](b: B, offset: O, v: V[T])(implicit rw: ReaderWriter[B, V, S, O]): S

  /**
    * Read an object from string
    */
  def readString(s: String)(implicit srw: StringReaderWriter): T
  def writeString(v: T)(implicit srw: StringReaderWriter): String
}

object Storable {

  implicit val booleanStorable: Storable[Boolean] = new Storable[Boolean] {
    override def read[B, V[_], S, O](b: B)(implicit rw: ReaderWriter[B, V, S, O]): V[Boolean] = rw.readBoolean(b)
    override def read[B, V[_], S, O](b: B, offset: O)(implicit rw: ReaderWriter[B, V, S, O]): V[Boolean] =
      rw.readBoolean(b, offset)
    override def write[B, V[_], S, O](b: B, v: V[Boolean])(implicit rw: ReaderWriter[B, V, S, O]): S =
      rw.writeBoolean(b, v)
    override def write[B, V[_], S, O](b: B, offset: O, v: V[Boolean])(implicit rw: ReaderWriter[B, V, S, O]): S =
      rw.writeBoolean(b, offset, v)

    override def readString(s: String)(implicit srw: StringReaderWriter): Boolean = srw.readBoolean(s)
    override def writeString(v: Boolean)(implicit srw: StringReaderWriter): String = srw.writeBoolean(v)
  }

  implicit val doubleStorable: Storable[Double] = new Storable[Double] {
    override def read[B, V[_], S, O](b: B)(implicit rw: ReaderWriter[B, V, S, O]): V[Double] = rw.readDouble(b)
    override def read[B, V[_], S, O](b: B, offset: O)(implicit rw: ReaderWriter[B, V, S, O]): V[Double] =
      rw.readDouble(b, offset)
    override def write[B, V[_], S, O](b: B, v: V[Double])(implicit rw: ReaderWriter[B, V, S, O]): S =
      rw.writeDouble(b, v)
    override def write[B, V[_], S, O](b: B, offset: O, v: V[Double])(implicit rw: ReaderWriter[B, V, S, O]): S =
      rw.writeDouble(b, offset, v)

    override def readString(s: String)(implicit srw: StringReaderWriter): Double = srw.readDouble(s)
    override def writeString(v: Double)(implicit srw: StringReaderWriter): String = srw.writeDouble(v)
  }

  implicit val bigDecimalStorable: Storable[BigDecimal] = new Storable[BigDecimal] {
    override def read[B, V[_], S, O](b: B)(implicit rw: ReaderWriter[B, V, S, O]): V[BigDecimal] = rw.readBigDecimal(b)
    override def read[B, V[_], S, O](b: B, offset: O)(implicit rw: ReaderWriter[B, V, S, O]): V[BigDecimal] =
      rw.readBigDecimal(b, offset)
    override def write[B, V[_], S, O](b: B, v: V[BigDecimal])(implicit rw: ReaderWriter[B, V, S, O]): S =
      rw.writeBigDecimal(b, v)
    override def write[B, V[_], S, O](b: B, offset: O, v: V[BigDecimal])(implicit rw: ReaderWriter[B, V, S, O]): S =
      rw.writeBigDecimal(b, offset, v)

    override def readString(s: String)(implicit srw: StringReaderWriter): BigDecimal = srw.readDecimal(s)
    override def writeString(v: BigDecimal)(implicit srw: StringReaderWriter): String = srw.writeDecimal(v)
  }

  implicit val byteStorable: Storable[Byte] = new Storable[Byte] {
    override def read[B, V[_], S, O](b: B)(implicit rw: ReaderWriter[B, V, S, O]): V[Byte] = rw.readByte(b)
    override def read[B, V[_], S, O](b: B, offset: O)(implicit rw: ReaderWriter[B, V, S, O]): V[Byte] =
      rw.readByte(b, offset)
    override def write[B, V[_], S, O](b: B, v: V[Byte])(implicit rw: ReaderWriter[B, V, S, O]): S =
      rw.writeByte(b, v)
    override def write[B, V[_], S, O](b: B, offset: O, v: V[Byte])(implicit rw: ReaderWriter[B, V, S, O]): S =
      rw.writeByte(b, offset, v)

    override def readString(s: String)(implicit srw: StringReaderWriter): Byte = srw.readByte(s)
    override def writeString(v: Byte)(implicit srw: StringReaderWriter): String = srw.writeByte(v)
  }

  implicit val shortStorable: Storable[Short] = new Storable[Short] {
    override def read[B, V[_], S, O](b: B)(implicit rw: ReaderWriter[B, V, S, O]): V[Short] = rw.readVShort(b)
    override def read[B, V[_], S, O](b: B, offset: O)(implicit rw: ReaderWriter[B, V, S, O]): V[Short] =
      rw.readVShort(b, offset)
    override def write[B, V[_], S, O](b: B, v: V[Short])(implicit rw: ReaderWriter[B, V, S, O]): S =
      rw.writeVShort(b, v)
    override def write[B, V[_], S, O](b: B, offset: O, v: V[Short])(implicit rw: ReaderWriter[B, V, S, O]): S =
      rw.writeVShort(b, offset, v)

    override def readString(s: String)(implicit srw: StringReaderWriter): Short = srw.readShort(s)
    override def writeString(v: Short)(implicit srw: StringReaderWriter): String = srw.writeShort(v)
  }

  implicit val intStorable: Storable[Int] = new Storable[Int] {
    override def read[B, V[_], S, O](b: B)(implicit rw: ReaderWriter[B, V, S, O]): V[Int] = rw.readVInt(b)
    override def read[B, V[_], S, O](b: B, offset: O)(implicit rw: ReaderWriter[B, V, S, O]): V[Int] =
      rw.readVInt(b, offset)
    override def write[B, V[_], S, O](b: B, v: V[Int])(implicit rw: ReaderWriter[B, V, S, O]): S =
      rw.writeVInt(b, v)
    override def write[B, V[_], S, O](b: B, offset: O, v: V[Int])(implicit rw: ReaderWriter[B, V, S, O]): S =
      rw.writeVInt(b, offset, v)

    override def readString(s: String)(implicit srw: StringReaderWriter): Int = srw.readInt(s)
    override def writeString(v: Int)(implicit srw: StringReaderWriter): String = srw.writeInt(v)
  }

  implicit val longStorable: Storable[Long] = new Storable[Long] {
    override def read[B, V[_], S, O](b: B)(implicit rw: ReaderWriter[B, V, S, O]): V[Long] = rw.readVLong(b)
    override def read[B, V[_], S, O](b: B, offset: O)(implicit rw: ReaderWriter[B, V, S, O]): V[Long] =
      rw.readVLong(b, offset)
    override def write[B, V[_], S, O](b: B, v: V[Long])(implicit rw: ReaderWriter[B, V, S, O]): S =
      rw.writeVLong(b, v)
    override def write[B, V[_], S, O](b: B, offset: O, v: V[Long])(implicit rw: ReaderWriter[B, V, S, O]): S =
      rw.writeVLong(b, offset, v)

    override def readString(s: String)(implicit srw: StringReaderWriter): Long = srw.readLong(s)
    override def writeString(v: Long)(implicit srw: StringReaderWriter): String = srw.writeLong(v)
  }

  implicit val stringStorable: Storable[String] = new Storable[String] {
    override def read[B, V[_], S, O](b: B)(implicit rw: ReaderWriter[B, V, S, O]): V[String] = rw.readString(b)
    override def read[B, V[_], S, O](b: B, offset: O)(implicit rw: ReaderWriter[B, V, S, O]): V[String] =
      rw.readString(b, offset)
    override def write[B, V[_], S, O](b: B, v: V[String])(implicit rw: ReaderWriter[B, V, S, O]): S =
      rw.writeString(b, v)
    override def write[B, V[_], S, O](b: B, offset: O, v: V[String])(implicit rw: ReaderWriter[B, V, S, O]): S =
      rw.writeString(b, offset, v)

    override def readString(s: String)(implicit srw: StringReaderWriter): String = srw.readString(s)
    override def writeString(v: String)(implicit srw: StringReaderWriter): String = srw.writeString(v)
  }

  implicit val timestampStorable: Storable[Time] = new Storable[Time] {
    override def read[B, V[_], S, O](b: B)(implicit rw: ReaderWriter[B, V, S, O]): V[Time] = rw.readVTime(b)
    override def read[B, V[_], S, O](b: B, offset: O)(implicit rw: ReaderWriter[B, V, S, O]): V[Time] =
      rw.readVTime(b, offset)
    override def write[B, V[_], S, O](b: B, v: V[Time])(implicit rw: ReaderWriter[B, V, S, O]): S =
      rw.writeVTime(b, v)
    override def write[B, V[_], S, O](b: B, offset: O, v: V[Time])(implicit rw: ReaderWriter[B, V, S, O]): S =
      rw.writeVTime(b, offset, v)

    override def readString(s: String)(implicit srw: StringReaderWriter): Time = srw.readTime(s)
    override def writeString(v: Time)(implicit srw: StringReaderWriter): String = srw.writeTime(v)
  }

  implicit val currencyStorable: Storable[Currency] = new Storable[Currency] {
    override def read[B, V[_], S, O](b: B)(implicit rw: ReaderWriter[B, V, S, O]): V[Currency] = rw.readVCurrency(b)
    override def read[B, V[_], S, O](b: B, offset: O)(implicit rw: ReaderWriter[B, V, S, O]): V[Currency] =
      rw.readVCurrency(b, offset)
    override def write[B, V[_], S, O](b: B, v: V[Currency])(implicit rw: ReaderWriter[B, V, S, O]): S =
      rw.writeVCurrency(b, v)
    override def write[B, V[_], S, O](b: B, offset: O, v: V[Currency])(implicit rw: ReaderWriter[B, V, S, O]): S =
      rw.writeVCurrency(b, offset, v)
    override def readString(s: String)(implicit srw: StringReaderWriter): Currency = srw.readCurrency(s)
    override def writeString(v: Currency)(implicit srw: StringReaderWriter): String = srw.writeCurrency(v)
  }

  implicit val periodStorable: Storable[PeriodDuration] = new Storable[PeriodDuration] {

    override def read[B, V[_], S, O](b: B)(implicit rw: ReaderWriter[B, V, S, O]): V[PeriodDuration] =
      rw.readPeriodDuration(b)
    override def read[B, V[_], S, O](b: B, offset: O)(implicit rw: ReaderWriter[B, V, S, O]): V[PeriodDuration] =
      rw.readPeriodDuration(b, offset)

    override def write[B, V[_], S, O](b: B, v: V[PeriodDuration])(implicit rw: ReaderWriter[B, V, S, O]): S =
      rw.writePeriodDuration(b, v)
    override def write[B, V[_], S, O](b: B, offset: O, v: V[PeriodDuration])(implicit rw: ReaderWriter[B, V, S, O]): S =
      rw.writePeriodDuration(b, offset, v)

    override def readString(s: String)(implicit srw: StringReaderWriter): PeriodDuration = srw.readPeriodDuration(s)
    override def writeString(v: PeriodDuration)(implicit srw: StringReaderWriter): String = srw.writePeriodDuration(v)
  }

  implicit val blobStorable: Storable[Blob] = new Storable[Blob] {
    override def read[B, V[_], S, O](b: B)(implicit rw: ReaderWriter[B, V, S, O]): V[Blob] = rw.readBlob(b)
    override def read[B, V[_], S, O](b: B, offset: O)(implicit rw: ReaderWriter[B, V, S, O]): V[Blob] =
      rw.readBlob(b, offset)
    override def write[B, V[_], S, O](b: B, v: V[Blob])(implicit rw: ReaderWriter[B, V, S, O]): S =
      rw.writeBlob(b, v)
    override def write[B, V[_], S, O](b: B, offset: O, v: V[Blob])(implicit rw: ReaderWriter[B, V, S, O]): S =
      rw.writeBlob(b, offset, v)

    override def readString(s: String)(implicit srw: StringReaderWriter): Blob = srw.readBlob(s)
    override def writeString(v: Blob)(implicit srw: StringReaderWriter): String = srw.writeBlob(v)
  }

  implicit def seqStorable[T](implicit tStorable: Storable[T], ct: ClassTag[T]): Storable[Seq[T]] = {
    new Storable[Seq[T]] {
      override def read[B, V[_], S, O](bb: B)(implicit rw: ReaderWriter[B, V, S, O]): V[Seq[T]] = {
        rw.readSeq(bb, b => tStorable.read(b)(rw))
      }

      override def read[B, V[_], S, O](bb: B, offset: O)(implicit rw: ReaderWriter[B, V, S, O]): V[Seq[T]] = {
        rw.readSeq(bb, offset, b => tStorable.read(b)(rw))
      }

      override def write[B, V[_], S, O](bb: B, v: V[Seq[T]])(implicit rw: ReaderWriter[B, V, S, O]): S = {
        rw.writeSeq(bb, v, (b, v: V[T]) => tStorable.write(b, v)(rw))
      }

      override def write[B, V[_], S, O](bb: B, offset: O, v: V[Seq[T]])(implicit rw: ReaderWriter[B, V, S, O]): S = {
        rw.writeSeq(bb, offset, v, (b, v: V[T]) => tStorable.write(b, v)(rw))
      }

      override def readString(s: String)(implicit srw: StringReaderWriter): Seq[T] = srw.readSeq(s)
      override def writeString(v: Seq[T])(implicit srw: StringReaderWriter): String = srw.writeSeq(v)
    }
  }

  def noop[T]: Storable[T] = new Storable[T] {
    override def read[B, V[_], S, O](b: B)(implicit rw: ReaderWriter[B, V, S, O]): V[T] = {
      throw new IllegalStateException("This should not be read")
    }

    override def read[B, V[_], S, O](b: B, offset: O)(implicit rw: ReaderWriter[B, V, S, O]): V[T] = {
      throw new IllegalStateException("This should not be read")
    }

    override def write[B, V[_], S, O](b: B, v: V[T])(implicit rw: ReaderWriter[B, V, S, O]): S = {
      throw new IllegalStateException("This should not be written")
    }

    override def write[B, V[_], S, O](b: B, offset: O, v: V[T])(implicit rw: ReaderWriter[B, V, S, O]): S = {
      throw new IllegalStateException("This should not be written")
    }

    override def readString(s: String)(implicit srw: StringReaderWriter): T = {
      throw new IllegalStateException("This should not be written")
    }

    override def writeString(v: T)(implicit srw: StringReaderWriter): String = {
      throw new IllegalStateException("This should not be written")
    }
  }

}
