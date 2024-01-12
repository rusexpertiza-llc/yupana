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

import org.yupana.api.Time

import scala.annotation.implicitNotFound

@implicitNotFound("No member of type class FixedStorable for class ${T} is found")
trait FixedStorable[T] extends Serializable {
  val size: Int

  def read[B, V[_], RW[_]](bb: B)(implicit rw: ReaderWriter[B, V, RW]): V[T]
  def read[B, V[_], RW[_]](b: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[T]

  def write[B, V[_], RW[_]](bb: B, v: V[T])(implicit rw: ReaderWriter[B, V, RW]): RW[T]

  def write[B, V[_], WR[_]](b: B, offset: Int, v: V[T])(implicit rw: ReaderWriter[B, V, WR]): WR[T]
}

object FixedStorable {

  def apply[T](implicit ev: FixedStorable[T]): FixedStorable[T] = ev

  implicit val intStorable: FixedStorable[Int] = new FixedStorable[Int] {
    override val size: Int = 4
    override def read[B, V[_], RW[_]](bb: B)(implicit rw: ReaderWriter[B, V, RW]): V[Int] = rw.readInt(bb)
    override def read[B, V[_], RW[_]](bb: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[Int] =
      rw.readInt(bb, offset)
    override def write[B, V[_], RW[_]](bb: B, v: V[Int])(implicit rw: ReaderWriter[B, V, RW]): RW[Int] =
      rw.writeInt(bb, v)
    override def write[B, V[_], RW[_]](bb: B, offset: Int, v: V[Int])(implicit rw: ReaderWriter[B, V, RW]): RW[Int] =
      rw.writeInt(bb, offset, v)
  }

  implicit val longStorable: FixedStorable[Long] = new FixedStorable[Long] {
    override val size: Int = 8
    override def read[B, V[_], RW[_]](bb: B)(implicit rw: ReaderWriter[B, V, RW]): V[Long] = rw.readLong(bb)
    override def read[B, V[_], RW[_]](bb: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[Long] =
      rw.readLong(bb, offset)
    override def write[B, V[_], RW[_]](bb: B, v: V[Long])(implicit rw: ReaderWriter[B, V, RW]): RW[Long] =
      rw.writeLong(bb, v)
    override def write[B, V[_], RW[_]](bb: B, offset: Int, v: V[Long])(implicit rw: ReaderWriter[B, V, RW]): RW[Long] =
      rw.writeLong(bb, offset, v)
  }

  implicit val doubleStorable: FixedStorable[Double] = new FixedStorable[Double] {
    override val size: Int = 8
    override def read[B, V[_], RW[_]](bb: B)(implicit rw: ReaderWriter[B, V, RW]): V[Double] = rw.readDouble(bb)
    override def read[B, V[_], RW[_]](bb: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[Double] =
      rw.readDouble(bb, offset)
    override def write[B, V[_], RW[_]](bb: B, v: V[Double])(implicit rw: ReaderWriter[B, V, RW]): RW[Double] =
      rw.writeDouble(bb, v)
    override def write[B, V[_], RW[_]](bb: B, offset: Int, v: V[Double])(
        implicit rw: ReaderWriter[B, V, RW]
    ): RW[Double] = rw.writeDouble(bb, offset, v)
  }

  implicit val shortStorable: FixedStorable[Short] = new FixedStorable[Short] {
    override val size: Int = 2
    override def read[B, V[_], RW[_]](bb: B)(implicit rw: ReaderWriter[B, V, RW]): V[Short] = rw.readShort(bb)
    override def read[B, V[_], RW[_]](bb: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[Short] =
      rw.readShort(bb, offset)
    override def write[B, V[_], RW[_]](bb: B, v: V[Short])(implicit rw: ReaderWriter[B, V, RW]): RW[Short] =
      rw.writeShort(bb, v)
    override def write[B, V[_], RW[_]](bb: B, offset: Int, v: V[Short])(
        implicit rw: ReaderWriter[B, V, RW]
    ): RW[Short] = rw.writeShort(bb, offset, v)
  }

  implicit val byteStorable: FixedStorable[Byte] = new FixedStorable[Byte] {
    override val size: Int = 1
    override def read[B, V[_], RW[_]](bb: B)(implicit rw: ReaderWriter[B, V, RW]): V[Byte] = rw.readByte(bb)
    override def read[B, V[_], RW[_]](bb: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[Byte] =
      rw.readByte(bb, offset)
    override def write[B, V[_], RW[_]](bb: B, v: V[Byte])(implicit rw: ReaderWriter[B, V, RW]): RW[Byte] =
      rw.writeByte(bb, v)
    override def write[B, V[_], RW[_]](bb: B, offset: Int, v: V[Byte])(implicit rw: ReaderWriter[B, V, RW]): RW[Byte] =
      rw.writeByte(bb, offset, v)
  }

  implicit val timeStorable: FixedStorable[Time] = new FixedStorable[Time] {
    override val size: Int = 8
    override def read[B, V[_], RW[_]](bb: B)(implicit rw: ReaderWriter[B, V, RW]): V[Time] = rw.readTime(bb)
    override def read[B, V[_], RW[_]](bb: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[Time] =
      rw.readTime(bb, offset)
    override def write[B, V[_], RW[_]](bb: B, v: V[Time])(implicit rw: ReaderWriter[B, V, RW]): RW[Time] =
      rw.writeTime(bb, v)
    override def write[B, V[_], RW[_]](bb: B, offset: Int, v: V[Time])(implicit rw: ReaderWriter[B, V, RW]): RW[Time] =
      rw.writeTime(bb, offset, v)
  }

  implicit def tupleStorable[T, U](
      implicit tStorable: FixedStorable[T],
      uStorable: FixedStorable[U]
  ): FixedStorable[(T, U)] = {
    new FixedStorable[(T, U)] {
      override val size: Int = tStorable.size + uStorable.size

      override def read[B, V[_], RW[_]](bb: B)(implicit rw: ReaderWriter[B, V, RW]): V[(T, U)] = {
        rw.readTuple(
          bb,
          b => tStorable.read(b)(rw),
          b => uStorable.read(b)(rw)
        )
      }

      override def read[B, V[_], RW[_]](bb: B, offset: Int)(implicit rw: ReaderWriter[B, V, RW]): V[(T, U)] = {
        rw.readTuple(bb, b => tStorable.read(b, offset)(rw), b => uStorable.read(b, offset + tStorable.size)(rw))

      }

      override def write[B, V[_], RW[_]](bb: B, v: V[(T, U)])(implicit rw: ReaderWriter[B, V, RW]): RW[(T, U)] = {
        rw.writeTuple(
          bb,
          v,
          (b, v) => tStorable.write(b, v)(rw),
          (b, v) => uStorable.write(b, v)(rw)
        )
      }

      override def write[B, V[_], RW[_]](bb: B, offset: Int, v: V[(T, U)])(
          implicit rw: ReaderWriter[B, V, RW]
      ): RW[(T, U)] = {
        rw.writeTuple(
          bb,
          v,
          (b, v) => tStorable.write(b, offset, v)(rw),
          (b, v) => uStorable.write(b, offset + tStorable.size, v)(rw)
        )
      }
    }
  }
}
