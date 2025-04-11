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

package org.yupana.protocol

import org.yupana.api.types.ByteReaderWriter

trait Write[T] { self =>

  /** Serialize data into buffer */
  def write[B: ByteReaderWriter](buf: B, t: T): Unit

  def map[A](from: A => T): Write[A] = new Write[A] {
    override def write[B: ByteReaderWriter](buf: B, t: A): Unit = self.write(buf, from(t))
  }
}

object Write {

  def apply[T](implicit ev: Write[T]): Write[T] = ev

  def product2[T, F1, F2](from: T => (F1, F2))(implicit wf1: Write[F1], wf2: Write[F2]): Write[T] = new Write[T] {
    override def write[B: ByteReaderWriter](buf: B, t: T): Unit = {
      val (f1, f2) = from(t)
      wf1.write(buf, f1)
      wf2.write(buf, f2)
    }
  }

  def product3[T, F1, F2, F3](
      from: T => (F1, F2, F3)
  )(implicit wf1: Write[F1], wf2: Write[F2], wf3: Write[F3]): Write[T] =
    new Write[T] {
      override def write[B: ByteReaderWriter](buf: B, t: T): Unit = {
        val (f1, f2, f3) = from(t)
        wf1.write(buf, f1)
        wf2.write(buf, f2)
        wf3.write(buf, f3)
      }
    }

  def product4[T, F1, F2, F3, F4](
      from: T => (F1, F2, F3, F4)
  )(implicit wf1: Write[F1], wf2: Write[F2], wf3: Write[F3], wf4: Write[F4]): Write[T] =
    new Write[T] {
      override def write[B: ByteReaderWriter](buf: B, t: T): Unit = {
        val (f1, f2, f3, f4) = from(t)
        wf1.write(buf, f1)
        wf2.write(buf, f2)
        wf3.write(buf, f3)
        wf4.write(buf, f4)
      }
    }

  implicit val writeBytes: Write[Array[Byte]] = new Write[Array[Byte]] {
    override def write[B](buf: B, t: Array[Byte])(implicit b: ByteReaderWriter[B]): Unit = {
      b.writeInt(buf, t.length)
      b.writeBytes(buf, t)
    }
  }

  implicit val writeByte: Write[Byte] = new Write[Byte] {
    override def write[B: ByteReaderWriter](buf: B, t: Byte): Unit = implicitly[ByteReaderWriter[B]].writeByte(buf, t)
  }

  implicit val writeInt: Write[Int] = new Write[Int] {
    override def write[B: ByteReaderWriter](buf: B, t: Int): Unit = implicitly[ByteReaderWriter[B]].writeInt(buf, t)
  }

  implicit val writeLong: Write[Long] = new Write[Long] {
    override def write[B](buf: B, t: Long)(implicit b: ByteReaderWriter[B]): Unit = b.writeLong(buf, t)
  }

  implicit val writeString: Write[String] = new Write[String] {
    override def write[B](buf: B, t: String)(implicit b: ByteReaderWriter[B]): Unit = b.writeString(buf, t)
  }

  implicit val writeBigDecimal: Write[BigDecimal] = implicitly[Write[String]].map(_.toString())

  implicit def writeOption[T](implicit rwt: Write[T]): Write[Option[T]] = new Write[Option[T]] {
    override def write[B](buf: B, t: Option[T])(implicit b: ByteReaderWriter[B]): Unit = {
      t match {
        case Some(t) =>
          b.writeByte(buf, 1)
          rwt.write(buf, t)
        case None => b.writeByte(buf, 0)
      }
    }
  }

  implicit def writeTuple[X, Y](implicit wx: Write[X], wy: Write[Y]): Write[(X, Y)] = product2[(X, Y), X, Y](identity)

  implicit def writeSeq[T](implicit rwt: Write[T]): Write[Seq[T]] = new Write[Seq[T]] {
    override def write[B](buf: B, t: Seq[T])(implicit b: ByteReaderWriter[B]): Unit = {
      b.writeInt(buf, t.length)
      t.foreach(x => rwt.write(buf, x))
    }
  }

  implicit def writeMap[K, V](implicit rwk: Write[K], rwv: Write[V]): Write[Map[K, V]] =
    implicitly[Write[Seq[(K, V)]]].map(_.toSeq)

}
