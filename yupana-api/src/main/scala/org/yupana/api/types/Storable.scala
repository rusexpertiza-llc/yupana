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
import org.yupana.api.{ Blob, Time }

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
  def read[B, V[_], RW[_]](b: B)(implicit rw: ReaderWriter[B, V, RW]): V[T]

  def read[B, V[_], RW[_]](b: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[T]

  /**
    * Serialize instance of T into array of bytes
    * @param t value to be serialized
    * @return serialized bytes
    */
  def write[B, V[_], WR[_]](b: B, v: V[T])(implicit rw: ReaderWriter[B, V, WR]): WR[T]

  def write[B, V[_], WR[_]](b: B, offset: Int, v: V[T])(implicit rw: ReaderWriter[B, V, WR]): WR[T]
}

object Storable {

  implicit val booleanStorable: Storable[Boolean] = new Storable[Boolean] {
    override def read[B, V[_], RW[_]](b: B)(implicit rw: ReaderWriter[B, V, RW]): V[Boolean] = rw.readBoolean(b)
    override def read[B, V[_], RW[_]](b: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[Boolean] =
      rw.readBoolean(b, offset)
    override def write[B, V[_], RW[_]](b: B, v: V[Boolean])(implicit rw: ReaderWriter[B, V, RW]): RW[Boolean] =
      rw.writeBoolean(b, v)
    override def write[B, V[_], RW[_]](b: B, offset: Int, v: V[Boolean])(
      implicit rw: ReaderWriter[B, V, RW]
    ): RW[Boolean] =
      rw.writeBoolean(b, offset, v)
  }

  implicit val doubleStorable: Storable[Double] = new Storable[Double] {
    override def read[B, V[_], RW[_]](b: B)(implicit rw: ReaderWriter[B, V, RW]): V[Double] = rw.readDouble(b)
    override def read[B, V[_], RW[_]](b: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[Double] =
      rw.readDouble(b, offset)
    override def write[B, V[_], RW[_]](b: B, v: V[Double])(implicit rw: ReaderWriter[B, V, RW]): RW[Double] =
      rw.writeDouble(b, v)
    override def write[B, V[_], RW[_]](b: B, offset: Int, v: V[Double])(
      implicit rw: ReaderWriter[B, V, RW]
    ): RW[Double] =
      rw.writeDouble(b, offset, v)
  }

  implicit val bigDecimalStorable: Storable[BigDecimal] = new Storable[BigDecimal] {
    override def read[B, V[_], RW[_]](b: B)(implicit rw: ReaderWriter[B, V, RW]): V[BigDecimal] = rw.readBigDecimal(b)
    override def read[B, V[_], RW[_]](b: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[BigDecimal] =
      rw.readBigDecimal(b, offset)
    override def write[B, V[_], RW[_]](b: B, v: V[BigDecimal])(implicit rw: ReaderWriter[B, V, RW]): RW[BigDecimal] =
      rw.writeBigDecimal(b, v)
    override def write[B, V[_], RW[_]](b: B, offset: Int, v: V[BigDecimal])(
      implicit rw: ReaderWriter[B, V, RW]
    ): RW[BigDecimal] =
      rw.writeBigDecimal(b, offset, v)

  }

  implicit val byteStorable: Storable[Byte] = new Storable[Byte] {
    override def read[B, V[_], RW[_]](b: B)(implicit rw: ReaderWriter[B, V, RW]): V[Byte] = rw.readByte(b)
    override def read[B, V[_], RW[_]](b: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[Byte] =
      rw.readByte(b, offset)
    override def write[B, V[_], RW[_]](b: B, v: V[Byte])(implicit rw: ReaderWriter[B, V, RW]): RW[Byte] =
      rw.writeByte(b, v)
    override def write[B, V[_], RW[_]](b: B, offset: Int, v: V[Byte])(implicit rw: ReaderWriter[B, V, RW]): RW[Byte] =
      rw.writeByte(b, offset, v)
  }
  implicit val shortStorable: Storable[Short] = new Storable[Short] {
    override def read[B, V[_], RW[_]](b: B)(implicit rw: ReaderWriter[B, V, RW]): V[Short] = rw.readVShort(b)
    override def read[B, V[_], RW[_]](b: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[Short] =
      rw.readVShort(b, offset)
    override def write[B, V[_], RW[_]](b: B, v: V[Short])(implicit rw: ReaderWriter[B, V, RW]): RW[Short] =
      rw.writeVShort(b, v)
    override def write[B, V[_], RW[_]](b: B, offset: Int, v: V[Short])(implicit rw: ReaderWriter[B, V, RW]): RW[Short] =
      rw.writeVShort(b, offset, v)
  }
  implicit val intStorable: Storable[Int] = new Storable[Int] {
    override def read[B, V[_], RW[_]](b: B)(implicit rw: ReaderWriter[B, V, RW]): V[Int] = rw.readVInt(b)
    override def read[B, V[_], RW[_]](b: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[Int] =
      rw.readVInt(b, offset)
    override def write[B, V[_], RW[_]](b: B, v: V[Int])(implicit rw: ReaderWriter[B, V, RW]): RW[Int] =
      rw.writeVInt(b, v)
    override def write[B, V[_], RW[_]](b: B, offset: Int, v: V[Int])(implicit rw: ReaderWriter[B, V, RW]): RW[Int] =
      rw.writeVInt(b, offset, v)
  }
  implicit val longStorable: Storable[Long] = new Storable[Long] {
    override def read[B, V[_], RW[_]](b: B)(implicit rw: ReaderWriter[B, V, RW]): V[Long] = rw.readVLong(b)
    override def read[B, V[_], RW[_]](b: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[Long] =
      rw.readVLong(b, offset)
    override def write[B, V[_], RW[_]](b: B, v: V[Long])(implicit rw: ReaderWriter[B, V, RW]): RW[Long] =
      rw.writeVLong(b, v)
    override def write[B, V[_], RW[_]](b: B, offset: Int, v: V[Long])(implicit rw: ReaderWriter[B, V, RW]): RW[Long] =
      rw.writeVLong(b, offset, v)
  }
  implicit val stringStorable: Storable[String] = new Storable[String] {
    override def read[B, V[_], RW[_]](b: B)(implicit rw: ReaderWriter[B, V, RW]): V[String] = rw.readString(b)
    override def read[B, V[_], RW[_]](b: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[String] =
      rw.readString(b, offset)
    override def write[B, V[_], RW[_]](b: B, v: V[String])(implicit rw: ReaderWriter[B, V, RW]): RW[String] =
      rw.writeString(b, v)
    override def write[B, V[_], RW[_]](b: B, offset: Int, v: V[String])(
      implicit rw: ReaderWriter[B, V, RW]
    ): RW[String] = rw.writeString(b, offset, v)
  }
  implicit val timestampStorable: Storable[Time] = new Storable[Time] {
    override def read[B, V[_], RW[_]](b: B)(implicit rw: ReaderWriter[B, V, RW]): V[Time] = rw.readVTime(b)
    override def read[B, V[_], RW[_]](b: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[Time] =
      rw.readVTime(b, offset)
    override def write[B, V[_], RW[_]](b: B, v: V[Time])(implicit rw: ReaderWriter[B, V, RW]): RW[Time] =
      rw.writeVTime(b, v)
    override def write[B, V[_], RW[_]](b: B, offset: Int, v: V[Time])(implicit rw: ReaderWriter[B, V, RW]): RW[Time] =
      rw.writeVTime(b, offset, v)
  }
  implicit val periodStorable: Storable[PeriodDuration] = new Storable[PeriodDuration] {

    override def read[B, V[_], RW[_]](b: B)(implicit rw: ReaderWriter[B, V, RW]): V[PeriodDuration] =
      rw.readPeriodDuration(b)
    override def read[B, V[_], RW[_]](b: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[PeriodDuration] =
      rw.readPeriodDuration(b, offset)

    override def write[B, V[_], RW[_]](b: B, v: V[PeriodDuration])(
      implicit rw: ReaderWriter[B, V, RW]
    ): RW[PeriodDuration] =
      rw.writePeriodDuration(b, v)
    override def write[B, V[_], RW[_]](b: B, offset: Int, v: V[PeriodDuration])(
      implicit rw: ReaderWriter[B, V, RW]
    ): RW[PeriodDuration] =
      rw.writePeriodDuration(b, offset, v)
  }

  implicit val blobStorable: Storable[Blob] = new Storable[Blob] {
    override def read[B, V[_], RW[_]](b: B)(implicit rw: ReaderWriter[B, V, RW]): V[Blob] = rw.readBlob(b)
    override def read[B, V[_], RW[_]](b: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[Blob] =
      rw.readBlob(b, offset)
    override def write[B, V[_], RW[_]](b: B, v: V[Blob])(implicit rw: ReaderWriter[B, V, RW]): RW[Blob] =
      rw.writeBlob(b, v)
    override def write[B, V[_], RW[_]](b: B, offset: Int, v: V[Blob])(implicit rw: ReaderWriter[B, V, RW]): RW[Blob] =
      rw.writeBlob(b, offset, v)
  }

  implicit def seqStorable[T](implicit tStorable: Storable[T], ct: ClassTag[T]): Storable[Seq[T]] = {
    new Storable[Seq[T]] {
      override def read[B, V[_], RW[_]](bb: B)(implicit rw: ReaderWriter[B, V, RW]): V[Seq[T]] = {
        rw.readSeq(bb, b => tStorable.read(b)(rw))
      }

      override def read[B, V[_], RW[_]](bb: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[Seq[T]] = {
        rw.readSeq(bb, offset, b => tStorable.read(b)(rw))
      }

      override def write[B, V[_], RW[_]](bb: B, v: V[Seq[T]])(implicit rw: ReaderWriter[B, V, RW]): RW[Seq[T]] = {
        rw.writeSeq(bb, v, (b, v) => tStorable.write(b, v)(rw))
      }

      override def write[B, V[_], RW[_]](bb: B, offset: Int, v: V[Seq[T]])(
        implicit rw: ReaderWriter[B, V, RW]
      ): RW[Seq[T]] = {
        rw.writeSeq(bb, offset, v, (b, v) => tStorable.write(b, v)(rw))
      }
    }
  }

  def noop[T]: Storable[T] = new Storable[T] {
    override def read[B, V[_], RW[_]](b: B)(implicit rw: ReaderWriter[B, V, RW]): V[T] = {
      throw new IllegalStateException("This should not be read")
    }

    override def read[B, V[_], RW[_]](b: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[T] = {
      throw new IllegalStateException("This should not be read")
    }

    override def write[B, V[_], RW[_]](b: B, v: V[T])(implicit rw: ReaderWriter[B, V, RW]): RW[T] = {
      throw new IllegalStateException("This should not be written")
    }

    override def write[B, V[_], RW[_]](b: B, offset: Int, v: V[T])(implicit rw: ReaderWriter[B, V, RW]): RW[T] = {
      throw new IllegalStateException("This should not be written")
    }
  }

}
