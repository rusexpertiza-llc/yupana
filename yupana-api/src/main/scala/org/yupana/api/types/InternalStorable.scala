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
  * Serialization/deserialization type class
  * @tparam T type to be serialized/deserialized
  */
@implicitNotFound("No member of type class Storable for class ${T} is found")
trait InternalStorable[T] extends Serializable {

  def size[V[_], S](v: V[T])(implicit rw: InternalReaderWriter[_, V, S, _]): S

  val fixedSize: Option[Int]

  val isRefType: Boolean

  /**
    * Reads an object from byte buffer
    * @param b byte buffer to read bytes
    * @return deserialized object
    */
  def read[B, V[_], S, O](b: B, size: S)(implicit rw: InternalReaderWriter[B, V, S, O]): V[T]
  def read[B, V[_], S, O](b: B, offset: O, size: S)(implicit rw: InternalReaderWriter[B, V, S, O]): V[T]

  /**
    * Serialize instance of T into array of bytes
    * @param t value to be serialized
    * @return serialized bytes
    */
  def write[B, V[_], S, O](b: B, v: V[T])(implicit rw: InternalReaderWriter[B, V, S, O]): S
  def write[B, V[_], S, O](b: B, offset: O, v: V[T])(implicit rw: InternalReaderWriter[B, V, S, O]): S

}

object InternalStorable {

  implicit val booleanStorable: InternalStorable[Boolean] = new InternalStorable[Boolean] {

    override val isRefType = false

    override def size[V[_], S](v: V[Boolean])(implicit rw: InternalReaderWriter[_, V, S, _]): S = rw.sizeOfBoolean

    override val fixedSize: Option[Int] = Some(1)

    override def read[B, V[_], S, O](b: B, size: S)(implicit rw: InternalReaderWriter[B, V, S, O]): V[Boolean] =
      rw.readBoolean(b)

    override def read[B, V[_], S, O](b: B, offset: O, size: S)(
        implicit rw: InternalReaderWriter[B, V, S, O]
    ): V[Boolean] =
      rw.readBoolean(b, offset)

    override def write[B, V[_], S, O](b: B, v: V[Boolean])(implicit rw: InternalReaderWriter[B, V, S, O]): S =
      rw.writeBoolean(b, v)

    override def write[B, V[_], S, O](b: B, offset: O, v: V[Boolean])(
        implicit rw: InternalReaderWriter[B, V, S, O]
    ): S =
      rw.writeBoolean(b, offset, v)
  }

  implicit val doubleStorable: InternalStorable[Double] = new InternalStorable[Double] {

    override val isRefType = false

    override def size[V[_], S](v: V[Double])(implicit rw: InternalReaderWriter[_, V, S, _]): S = rw.sizeOfDouble

    override val fixedSize: Option[Int] = Some(8)

    override def read[B, V[_], S, O](b: B, size: S)(implicit rw: InternalReaderWriter[B, V, S, O]): V[Double] =
      rw.readDouble(b)

    override def read[B, V[_], S, O](b: B, offset: O, size: S)(
        implicit rw: InternalReaderWriter[B, V, S, O]
    ): V[Double] =
      rw.readDouble(b, offset)

    override def write[B, V[_], S, O](b: B, v: V[Double])(implicit rw: InternalReaderWriter[B, V, S, O]): S =
      rw.writeDouble(b, v)

    override def write[B, V[_], S, O](b: B, offset: O, v: V[Double])(
        implicit rw: InternalReaderWriter[B, V, S, O]
    ): S = rw.writeDouble(b, offset, v)
  }

  implicit val bigDecimalStorable: InternalStorable[BigDecimal] = new InternalStorable[BigDecimal] {

    override val isRefType = true

    override def size[V[_], S](v: V[BigDecimal])(implicit rw: InternalReaderWriter[_, V, S, _]): S =
      rw.sizeOfBigDecimalSizeSpecified(v)

    override val fixedSize: Option[Int] = None

    override def read[B, V[_], S, O](b: B, size: S)(implicit rw: InternalReaderWriter[B, V, S, O]): V[BigDecimal] =
      rw.readBigDecimalSizeSpecified(b, size)

    override def read[B, V[_], S, O](b: B, offset: O, size: S)(
        implicit rw: InternalReaderWriter[B, V, S, O]
    ): V[BigDecimal] =
      rw.readBigDecimalSizeSpecified(b, offset, size)

    override def write[B, V[_], S, O](b: B, v: V[BigDecimal])(implicit rw: InternalReaderWriter[B, V, S, O]): S =
      rw.writeBigDecimalSizeSpecified(b, v)

    override def write[B, V[_], S, O](b: B, offset: O, v: V[BigDecimal])(
        implicit rw: InternalReaderWriter[B, V, S, O]
    ): S =
      rw.writeBigDecimalSizeSpecified(b, offset, v)
  }

  implicit val byteStorable: InternalStorable[Byte] = new InternalStorable[Byte] {

    override val isRefType = false

    override def size[V[_], S](v: V[Byte])(implicit rw: InternalReaderWriter[_, V, S, _]): S = rw.sizeOfByte

    override val fixedSize: Option[Int] = Some(1)

    override def read[B, V[_], S, O](b: B, size: S)(implicit rw: InternalReaderWriter[B, V, S, O]): V[Byte] =
      rw.readByte(b)

    override def read[B, V[_], S, O](b: B, offset: O, size: S)(implicit rw: InternalReaderWriter[B, V, S, O]): V[Byte] =
      rw.readByte(b, offset)

    override def write[B, V[_], S, O](b: B, v: V[Byte])(implicit rw: InternalReaderWriter[B, V, S, O]): S =
      rw.writeByte(b, v)

    override def write[B, V[_], S, O](b: B, offset: O, v: V[Byte])(implicit rw: InternalReaderWriter[B, V, S, O]): S =
      rw.writeByte(b, offset, v)
  }

  implicit val shortStorable: InternalStorable[Short] = new InternalStorable[Short] {

    override val isRefType = false

    override def size[V[_], S](v: V[Short])(implicit rw: InternalReaderWriter[_, V, S, _]): S = rw.sizeOfShort

    override val fixedSize: Option[Int] = Some(2)

    override def read[B, V[_], S, O](b: B, size: S)(implicit rw: InternalReaderWriter[B, V, S, O]): V[Short] =
      rw.readShort(b)

    override def read[B, V[_], S, O](b: B, offset: O, size: S)(
        implicit rw: InternalReaderWriter[B, V, S, O]
    ): V[Short] =
      rw.readShort(b, offset)

    override def write[B, V[_], S, O](b: B, v: V[Short])(implicit rw: InternalReaderWriter[B, V, S, O]): S =
      rw.writeShort(b, v)

    override def write[B, V[_], S, O](b: B, offset: O, v: V[Short])(implicit rw: InternalReaderWriter[B, V, S, O]): S =
      rw.writeShort(b, offset, v)
  }

  implicit val intStorable: InternalStorable[Int] = new InternalStorable[Int] {

    override val isRefType = false

    override def size[V[_], S](v: V[Int])(implicit rw: InternalReaderWriter[_, V, S, _]): S = rw.sizeOfInt

    override val fixedSize: Option[Int] = Some(4)

    override def read[B, V[_], S, O](b: B, size: S)(implicit rw: InternalReaderWriter[B, V, S, O]): V[Int] =
      rw.readInt(b)

    override def read[B, V[_], S, O](b: B, offset: O, size: S)(implicit rw: InternalReaderWriter[B, V, S, O]): V[Int] =
      rw.readInt(b, offset)

    override def write[B, V[_], S, O](b: B, v: V[Int])(implicit rw: InternalReaderWriter[B, V, S, O]): S =
      rw.writeInt(b, v)

    override def write[B, V[_], S, O](b: B, offset: O, v: V[Int])(implicit rw: InternalReaderWriter[B, V, S, O]): S =
      rw.writeInt(b, offset, v)
  }

  implicit val longStorable: InternalStorable[Long] = new InternalStorable[Long] {

    override val isRefType = false

    override def size[V[_], S](v: V[Long])(implicit rw: InternalReaderWriter[_, V, S, _]): S = rw.sizeOfLong

    override val fixedSize: Option[Int] = Some(8)

    override def read[B, V[_], S, O](b: B, size: S)(implicit rw: InternalReaderWriter[B, V, S, O]): V[Long] =
      rw.readLong(b)

    override def read[B, V[_], S, O](b: B, offset: O, size: S)(implicit rw: InternalReaderWriter[B, V, S, O]): V[Long] =
      rw.readLong(b, offset)

    override def write[B, V[_], S, O](b: B, v: V[Long])(implicit rw: InternalReaderWriter[B, V, S, O]): S =
      rw.writeLong(b, v)

    override def write[B, V[_], S, O](b: B, offset: O, v: V[Long])(implicit rw: InternalReaderWriter[B, V, S, O]): S =
      rw.writeLong(b, offset, v)
  }

  implicit val stringStorable: InternalStorable[String] = new InternalStorable[String] {

    override val isRefType = true

    override def size[V[_], S](v: V[String])(implicit rw: InternalReaderWriter[_, V, S, _]): S = rw.sizeOfStringSizeSpecified(v)

    override val fixedSize: Option[Int] = None

    override def read[B, V[_], S, O](b: B, size: S)(implicit rw: InternalReaderWriter[B, V, S, O]): V[String] =
      rw.readStringSizeSpecified(b, size)

    override def read[B, V[_], S, O](b: B, offset: O, size: S)(
        implicit rw: InternalReaderWriter[B, V, S, O]
    ): V[String] =
      rw.readStringSizeSpecified(b, offset, size)

    override def write[B, V[_], S, O](b: B, v: V[String])(implicit rw: InternalReaderWriter[B, V, S, O]): S =
      rw.writeStringSizeSpecified(b, v)

    override def write[B, V[_], S, O](b: B, offset: O, v: V[String])(
        implicit rw: InternalReaderWriter[B, V, S, O]
    ): S = rw.writeStringSizeSpecified(b, offset, v)
  }

  implicit val timestampStorable: InternalStorable[Time] = new InternalStorable[Time] {

    override val isRefType = false

    override def size[V[_], S](v: V[Time])(implicit rw: InternalReaderWriter[_, V, S, _]): S = rw.sizeOfTime

    override val fixedSize: Option[Int] = Some(8)

    override def read[B, V[_], S, O](b: B, size: S)(implicit rw: InternalReaderWriter[B, V, S, O]): V[Time] =
      rw.readTime(b)

    override def read[B, V[_], S, O](b: B, offset: O, size: S)(implicit rw: InternalReaderWriter[B, V, S, O]): V[Time] =
      rw.readTime(b, offset)

    override def write[B, V[_], S, O](b: B, v: V[Time])(implicit rw: InternalReaderWriter[B, V, S, O]): S =
      rw.writeTime(b, v)

    override def write[B, V[_], S, O](b: B, offset: O, v: V[Time])(implicit rw: InternalReaderWriter[B, V, S, O]): S =
      rw.writeTime(b, offset, v)
  }

  implicit val periodStorable: InternalStorable[PeriodDuration] = new InternalStorable[PeriodDuration] {

    override val isRefType = true

    override def size[V[_], S](v: V[PeriodDuration])(implicit rw: InternalReaderWriter[_, V, S, _]): S =
      rw.sizeOfPeriodDuration(v)

    override val fixedSize: Option[Int] = None

    override def read[B, V[_], S, O](b: B, size: S)(implicit rw: InternalReaderWriter[B, V, S, O]): V[PeriodDuration] =
      rw.readPeriodDuration(b)

    override def read[B, V[_], S, O](b: B, offset: O, size: S)(
        implicit rw: InternalReaderWriter[B, V, S, O]
    ): V[PeriodDuration] = {
      rw.readPeriodDuration(b, offset)
    }

    override def write[B, V[_], S, O](b: B, v: V[PeriodDuration])(
        implicit rw: InternalReaderWriter[B, V, S, O]
    ): S = {
      rw.writePeriodDuration(b, v)
    }

    override def write[B, V[_], S, O](b: B, offset: O, v: V[PeriodDuration])(
        implicit rw: InternalReaderWriter[B, V, S, O]
    ): S = {
      rw.writePeriodDuration(b, offset, v)
    }
  }

  implicit val blobStorable: InternalStorable[Blob] = new InternalStorable[Blob] {

    override val isRefType = true

    override def size[V[_], S](v: V[Blob])(implicit rw: InternalReaderWriter[_, V, S, _]): S = rw.sizeOfBlobSizeSpecified(v)

    override val fixedSize: Option[Int] = None

    override def read[B, V[_], S, O](b: B, size: S)(implicit rw: InternalReaderWriter[B, V, S, O]): V[Blob] =
      rw.readBlobSizeSpecified(b, size)

    override def read[B, V[_], S, O](b: B, offset: O, size: S)(implicit rw: InternalReaderWriter[B, V, S, O]): V[Blob] =
      rw.readBlobSizeSpecified(b, offset, size)

    override def write[B, V[_], S, O](b: B, v: V[Blob])(implicit rw: InternalReaderWriter[B, V, S, O]): S =
      rw.writeBlobSizeSpecified(b, v)

    override def write[B, V[_], S, O](b: B, offset: O, v: V[Blob])(implicit rw: InternalReaderWriter[B, V, S, O]): S =
      rw.writeBlobSizeSpecified(b, offset, v)
  }

  implicit def seqStorable[T](implicit tStorable: InternalStorable[T], ct: ClassTag[T]): InternalStorable[Seq[T]] = {
    new InternalStorable[Seq[T]] {

      override val isRefType = true

      override def size[V[_], S](v: V[Seq[T]])(implicit rw: InternalReaderWriter[_, V, S, _]): S = {
        rw.sizeOfSeqSizeSpecified(v, (x: V[T]) => tStorable.size(x)(rw))
      }

      override val fixedSize: Option[Int] = None

      override def read[B, V[_], S, O](bb: B, size: S)(implicit rw: InternalReaderWriter[B, V, S, O]): V[Seq[T]] = {
        rw.readSeqSizeSpecified(bb, (b, s) => tStorable.read(b, s)(rw))
      }

      override def read[B, V[_], S, O](bb: B, offset: O, size: S)(
          implicit rw: InternalReaderWriter[B, V, S, O]
      ): V[Seq[T]] = {
        rw.readSeqSizeSpecified(bb, offset, (b, s) => tStorable.read(b, s)(rw))
      }

      override def write[B, V[_], S, O](bb: B, v: V[Seq[T]])(implicit rw: InternalReaderWriter[B, V, S, O]): S = {
        rw.writeSeqSizeSpecified(bb, v, (b, vv: V[T]) => tStorable.write(b, vv)(rw))
      }

      override def write[B, V[_], S, O](bb: B, offset: O, v: V[Seq[T]])(
          implicit rw: InternalReaderWriter[B, V, S, O]
      ): S = {
        rw.writeSeqSizeSpecified(bb, offset, v, (b, vv: V[T]) => tStorable.write(b, vv)(rw))
      }
    }
  }

  implicit def tupleStorable[T, U](
      implicit tStorable: InternalStorable[T],
      uStorable: InternalStorable[U]
  ): InternalStorable[(T, U)] = {
    new InternalStorable[(T, U)] {

      override val isRefType = tStorable.isRefType || uStorable.isRefType

      override def size[V[_], S](v: V[(T, U)])(implicit rw: InternalReaderWriter[_, V, S, _]): S = {
        rw.sizeOfTuple(v, (t: V[T]) => tStorable.size(t), (u: V[U]) => uStorable.size(u))
      }

      override val fixedSize: Option[Int] = tStorable.fixedSize.flatMap(ts => uStorable.fixedSize.map(us => ts + us))

      override def read[B, V[_], S, O](bb: B, size: S)(implicit rw: InternalReaderWriter[B, V, S, O]): V[(T, U)] = {
        rw.readTupleSizeSpecified(bb, (b, s) => tStorable.read(b, s)(rw), (b, s) => uStorable.read(b, s)(rw))
      }

      override def read[B, V[_], S, O](bb: B, offset: O, size: S)(
          implicit rw: InternalReaderWriter[B, V, S, O]
      ): V[(T, U)] = {
        rw.readTupleSizeSpecified(bb, offset, (b, s) => tStorable.read(b, s)(rw), (b, s) => uStorable.read(b, s)(rw))
      }

      override def write[B, V[_], S, O](bb: B, v: V[(T, U)])(implicit rw: InternalReaderWriter[B, V, S, O]): S = {
        rw.writeTupleSizeSpecified(bb, v, (b, v: V[T]) => tStorable.write(b, v)(rw), (b, v: V[U]) => uStorable.write(b, v))
      }

      override def write[B, V[_], S, O](bb: B, offset: O, v: V[(T, U)])(
          implicit rw: InternalReaderWriter[B, V, S, O]
      ): S = {
        rw.writeTupleSizeSpecified(
          bb,
          offset,
          v,
          (b, v: V[T]) => tStorable.write(b, v)(rw),
          (b, v: V[U]) => uStorable.write(b, v)
        )
      }
    }
  }

  def noop[T]: InternalStorable[T] = new InternalStorable[T] {

    override val isRefType = false

    override def size[V[_], S](v: V[T])(implicit rw: InternalReaderWriter[_, V, S, _]): S = {
      throw new IllegalStateException("This can't have length")
    }

    override val fixedSize: Option[Int] = None

    override def read[B, V[_], S, O](b: B, size: S)(implicit rw: InternalReaderWriter[B, V, S, O]): V[T] = {
      throw new IllegalStateException("This should not be read")
    }

    override def read[B, V[_], S, O](b: B, offset: O, size: S)(implicit rw: InternalReaderWriter[B, V, S, O]): V[T] = {
      throw new IllegalStateException("This should not be read")
    }

    override def write[B, V[_], S, O](b: B, v: V[T])(implicit rw: InternalReaderWriter[B, V, S, O]): S = {
      throw new IllegalStateException("This should not be written")
    }

    override def write[B, V[_], S, O](b: B, offset: O, v: V[T])(implicit rw: InternalReaderWriter[B, V, S, O]): S = {
      throw new IllegalStateException("This should not be written")
    }
  }
}
