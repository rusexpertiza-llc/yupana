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

package org.yupana.serialization

import org.yupana.api.types.ByteReaderWriter

trait Read[T] { self =>

  /** Deserialize data from buffer */
  def read[B: ByteReaderWriter](buf: B): T

  def map[A](to: T => A): Read[A] = new Read[A] {
    override def read[B: ByteReaderWriter](buf: B): A = to(self.read(buf))
  }
}

object Read {

  def product2[T, F1, F2](to: (F1, F2) => T)(implicit rf1: Read[F1], rf2: Read[F2]): Read[T] = new Read[T] {
    override def read[B: ByteReaderWriter](buf: B): T = to(rf1.read(buf), rf2.read(buf))
  }

  def product3[T, F1, F2, F3](
      to: (F1, F2, F3) => T
  )(implicit rf1: Read[F1], rf2: Read[F2], rf3: Read[F3]): Read[T] = new Read[T] {
    override def read[B: ByteReaderWriter](buf: B): T = to(rf1.read(buf), rf2.read(buf), rf3.read(buf))
  }

  def product4[T, F1, F2, F3, F4](
      to: (F1, F2, F3, F4) => T
  )(implicit rf1: Read[F1], rwf2: Read[F2], rwf3: Read[F3], rwf4: Read[F4]): Read[T] = new Read[T] {
    override def read[B: ByteReaderWriter](buf: B): T =
      to(rf1.read(buf), rwf2.read(buf), rwf3.read(buf), rwf4.read(buf))
  }

  implicit val readBytes: Read[Array[Byte]] = new Read[Array[Byte]] {
    override def read[B](buf: B)(implicit b: ByteReaderWriter[B]): Array[Byte] = {
      val length = b.readInt(buf)
      val result = new Array[Byte](length)
      b.readBytes(buf, result)
      result
    }
  }

  implicit val readByte: Read[Byte] = new Read[Byte] {
    override def read[B: ByteReaderWriter](buf: B): Byte = implicitly[ByteReaderWriter[B]].readByte(buf)
  }

  implicit val readInt: Read[Int] = new Read[Int] {
    override def read[B: ByteReaderWriter](buf: B): Int = implicitly[ByteReaderWriter[B]].readInt(buf)
  }

  implicit val readLong: Read[Long] = new Read[Long] {
    override def read[B](buf: B)(implicit b: ByteReaderWriter[B]): Long = b.readLong(buf)
  }

  implicit val readString: Read[String] = new Read[String] {
    override def read[B](buf: B)(implicit b: ByteReaderWriter[B]): String = b.readString(buf)
  }

  implicit val readBigDecimal: Read[BigDecimal] = implicitly[Read[String]].map(BigDecimal.apply)

  implicit def readOption[T](implicit rwt: Read[T]): Read[Option[T]] = new Read[Option[T]] {
    override def read[B](buf: B)(implicit b: ByteReaderWriter[B]): Option[T] = {
      if (b.readByte(buf) == 1) Some(rwt.read(buf)) else None
    }
  }

  implicit def readTuple[X, Y](implicit rwx: Read[X], rwy: Read[Y]): Read[(X, Y)] = product2[(X, Y), X, Y](Tuple2.apply)

  implicit def readSeq[T](implicit rwt: Read[T]): Read[Seq[T]] = new Read[Seq[T]] {
    override def read[B](buf: B)(implicit b: ByteReaderWriter[B]): Seq[T] = {
      val len = b.readInt(buf)
      (0 until len).map { _ =>
        rwt.read(buf)
      }
    }
  }

  implicit def readMap[K, V](implicit rwk: Read[K], rwv: Read[V]): Read[Map[K, V]] =
    implicitly[Read[Seq[(K, V)]]].map(_.toMap)
}
