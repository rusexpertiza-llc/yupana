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
trait InternalStorable[T] extends Serializable {

  val fixedSize: Option[Int]

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

object InternalStorable {

  implicit val booleanStorable: InternalStorable[Boolean] = new InternalStorable[Boolean] {
    override val fixedSize: Option[Int] = Some(1)
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

  implicit val doubleStorable: InternalStorable[Double] = new InternalStorable[Double] {
    override val fixedSize: Option[Int] = Some(8)
    override def read[B, V[_], RW[_]](b: B)(implicit rw: ReaderWriter[B, V, RW]): V[Double] = rw.readDouble(b)
    override def read[B, V[_], RW[_]](b: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[Double] =
      rw.readDouble(b, offset)
    override def write[B, V[_], RW[_]](b: B, v: V[Double])(implicit rw: ReaderWriter[B, V, RW]): RW[Double] =
      rw.writeDouble(b, v)
    override def write[B, V[_], RW[_]](b: B, offset: Int, v: V[Double])(
        implicit rw: ReaderWriter[B, V, RW]
    ): RW[Double] = rw.writeDouble(b, offset, v)
  }

  implicit val bigDecimalStorable: InternalStorable[BigDecimal] = new InternalStorable[BigDecimal] {

    override val fixedSize: Option[Int] = None

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

  implicit val byteStorable: InternalStorable[Byte] = new InternalStorable[Byte] {

    override val fixedSize: Option[Int] = Some(1)

    override def read[B, V[_], RW[_]](b: B)(implicit rw: ReaderWriter[B, V, RW]): V[Byte] = rw.readByte(b)

    override def read[B, V[_], RW[_]](b: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[Byte] =
      rw.readByte(b, offset)

    override def write[B, V[_], RW[_]](b: B, v: V[Byte])(implicit rw: ReaderWriter[B, V, RW]): RW[Byte] =
      rw.writeByte(b, v)

    override def write[B, V[_], RW[_]](b: B, offset: Int, v: V[Byte])(implicit rw: ReaderWriter[B, V, RW]): RW[Byte] =
      rw.writeByte(b, offset, v)
  }

  implicit val shortStorable: InternalStorable[Short] = new InternalStorable[Short] {

    override val fixedSize: Option[Int] = Some(2)

    override def read[B, V[_], RW[_]](b: B)(implicit rw: ReaderWriter[B, V, RW]): V[Short] = rw.readShort(b)

    override def read[B, V[_], RW[_]](b: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[Short] =
      rw.readShort(b, offset)

    override def write[B, V[_], RW[_]](b: B, v: V[Short])(implicit rw: ReaderWriter[B, V, RW]): RW[Short] =
      rw.writeShort(b, v)

    override def write[B, V[_], RW[_]](b: B, offset: Int, v: V[Short])(implicit rw: ReaderWriter[B, V, RW]): RW[Short] =
      rw.writeShort(b, offset, v)
  }

  implicit val intStorable: InternalStorable[Int] = new InternalStorable[Int] {

    override val fixedSize: Option[Int] = Some(4)

    override def read[B, V[_], RW[_]](b: B)(implicit rw: ReaderWriter[B, V, RW]): V[Int] = rw.readInt(b)

    override def read[B, V[_], RW[_]](b: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[Int] =
      rw.readInt(b, offset)

    override def write[B, V[_], RW[_]](b: B, v: V[Int])(implicit rw: ReaderWriter[B, V, RW]): RW[Int] =
      rw.writeInt(b, v)

    override def write[B, V[_], RW[_]](b: B, offset: Int, v: V[Int])(implicit rw: ReaderWriter[B, V, RW]): RW[Int] =
      rw.writeInt(b, offset, v)
  }

  implicit val longStorable: InternalStorable[Long] = new InternalStorable[Long] {

    override val fixedSize: Option[Int] = Some(8)

    override def read[B, V[_], RW[_]](b: B)(implicit rw: ReaderWriter[B, V, RW]): V[Long] = rw.readLong(b)

    override def read[B, V[_], RW[_]](b: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[Long] =
      rw.readLong(b, offset)

    override def write[B, V[_], RW[_]](b: B, v: V[Long])(implicit rw: ReaderWriter[B, V, RW]): RW[Long] =
      rw.writeLong(b, v)

    override def write[B, V[_], RW[_]](b: B, offset: Int, v: V[Long])(implicit rw: ReaderWriter[B, V, RW]): RW[Long] =
      rw.writeLong(b, offset, v)
  }

  implicit val stringStorable: InternalStorable[String] = new InternalStorable[String] {

    override val fixedSize: Option[Int] = None

    override def read[B, V[_], RW[_]](b: B)(implicit rw: ReaderWriter[B, V, RW]): V[String] = rw.readString(b)

    override def read[B, V[_], RW[_]](b: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[String] =
      rw.readString(b, offset)

    override def write[B, V[_], RW[_]](b: B, v: V[String])(implicit rw: ReaderWriter[B, V, RW]): RW[String] =
      rw.writeString(b, v)

    override def write[B, V[_], RW[_]](b: B, offset: Int, v: V[String])(
        implicit rw: ReaderWriter[B, V, RW]
    ): RW[String] = rw.writeString(b, offset, v)
  }

  implicit val timestampStorable: InternalStorable[Time] = new InternalStorable[Time] {

    override val fixedSize: Option[Int] = Some(8)

    override def read[B, V[_], RW[_]](b: B)(implicit rw: ReaderWriter[B, V, RW]): V[Time] = rw.readTime(b)

    override def read[B, V[_], RW[_]](b: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[Time] =
      rw.readTime(b, offset)

    override def write[B, V[_], RW[_]](b: B, v: V[Time])(implicit rw: ReaderWriter[B, V, RW]): RW[Time] =
      rw.writeTime(b, v)

    override def write[B, V[_], RW[_]](b: B, offset: Int, v: V[Time])(implicit rw: ReaderWriter[B, V, RW]): RW[Time] =
      rw.writeTime(b, offset, v)
  }

  implicit val periodStorable: InternalStorable[PeriodDuration] = new InternalStorable[PeriodDuration] {

    override val fixedSize: Option[Int] = None

    override def read[B, V[_], RW[_]](b: B)(implicit rw: ReaderWriter[B, V, RW]): V[PeriodDuration] =
      rw.readPeriodDuration(b)

    override def read[B, V[_], RW[_]](b: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[PeriodDuration] = {
      rw.readPeriodDuration(b, offset)
    }

    override def write[B, V[_], RW[_]](b: B, v: V[PeriodDuration])(
        implicit rw: ReaderWriter[B, V, RW]
    ): RW[PeriodDuration] = {
      rw.writePeriodDuration(b, v)
    }

    override def write[B, V[_], RW[_]](b: B, offset: Int, v: V[PeriodDuration])(
        implicit rw: ReaderWriter[B, V, RW]
    ): RW[PeriodDuration] = {
      rw.writePeriodDuration(b, offset, v)
    }
  }

  implicit val blobStorable: InternalStorable[Blob] = new InternalStorable[Blob] {

    override val fixedSize: Option[Int] = None

    override def read[B, V[_], RW[_]](b: B)(implicit rw: ReaderWriter[B, V, RW]): V[Blob] = rw.readBlob(b)

    override def read[B, V[_], RW[_]](b: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[Blob] =
      rw.readBlob(b, offset)

    override def write[B, V[_], RW[_]](b: B, v: V[Blob])(implicit rw: ReaderWriter[B, V, RW]): RW[Blob] =
      rw.writeBlob(b, v)

    override def write[B, V[_], RW[_]](b: B, offset: Int, v: V[Blob])(implicit rw: ReaderWriter[B, V, RW]): RW[Blob] =
      rw.writeBlob(b, offset, v)
  }

  implicit def seqStorable[T](implicit tStorable: InternalStorable[T], ct: ClassTag[T]): InternalStorable[Seq[T]] = {
    new InternalStorable[Seq[T]] {

      override val fixedSize: Option[Int] = None

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

  implicit def tupleStorable[T, U](
      implicit tStorable: InternalStorable[T],
      uStorable: InternalStorable[U]
  ): InternalStorable[(T, U)] = {
    new InternalStorable[(T, U)] {

      override val fixedSize: Option[Int] = tStorable.fixedSize.flatMap(ts => uStorable.fixedSize.map(us => ts + us))

      override def read[B, V[_], RW[_]](bb: B)(implicit rw: ReaderWriter[B, V, RW]): V[(T, U)] = {
        rw.readTuple(bb, b => tStorable.read(b)(rw), b => uStorable.read(b)(rw))
      }

      override def read[B, V[_], RW[_]](bb: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[(T, U)] = {
        rw.readTuple(bb, offset, b => tStorable.read(b)(rw), b => uStorable.read(b)(rw))
      }

      override def write[B, V[_], RW[_]](bb: B, v: V[(T, U)])(implicit rw: ReaderWriter[B, V, RW]): RW[(T, U)] = {
        rw.writeTuple(bb, v, (b, v) => tStorable.write(b, v)(rw), (b, v) => uStorable.write(b, v))
      }

      override def write[B, V[_], RW[_]](bb: B, offset: Int, v: V[(T, U)])(
          implicit rw: ReaderWriter[B, V, RW]
      ): RW[(T, U)] = {
        rw.writeTuple(
          bb,
          offset,
          v,
          (b, v) => tStorable.write(b, v)(rw),
          (b, v) => uStorable.write(b, v)
        )
      }
    }
  }

  def noop[T]: InternalStorable[T] = new InternalStorable[T] {
    override val fixedSize: Option[Int] = None

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
