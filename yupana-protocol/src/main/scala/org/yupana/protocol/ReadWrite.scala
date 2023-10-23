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

trait ReadWrite[T] { self =>
  def read[B: Buffer](buf: B): T
  def write[B: Buffer](buf: B, t: T): Unit

  def imap[A](f: T => A)(g: A => T): ReadWrite[A] = new ReadWrite[A] {
    override def read[B: Buffer](buf: B): A = f(self.read(buf))
    override def write[B: Buffer](buf: B, t: A): Unit = self.write(buf, g(t))
  }
}

object ReadWrite {
//  implicit val rwBytes: ReadWrite[Array[Byte]] = new ReadWrite[Array[Byte]] {
//    override def read[B: Buffer](buf: B): Array[Byte] = implicitly[Buffer[B]].read(buf)
//
//    override def write[B: Buffer](buf: B, t: Array[Byte]): Unit = implicitly[Buffer[B]].write(buf, t)
//  }

  implicit val rwInt: ReadWrite[Int] = new ReadWrite[Int] {
    override def read[B: Buffer](buf: B): Int = implicitly[Buffer[B]].readInt(buf)
    override def write[B: Buffer](buf: B, t: Int): Unit = implicitly[Buffer[B]].writeInt(buf, t)
  }

  implicit val rwLong: ReadWrite[Long] = new ReadWrite[Long] {
    override def read[B](buf: B)(implicit b: Buffer[B]): Long = b.readLong(buf)
    override def write[B](buf: B, t: Long)(implicit b: Buffer[B]): Unit = b.writeLong(buf, t)
  }

  implicit val rwString: ReadWrite[String] = new ReadWrite[String] {
    override def read[B](buf: B)(implicit b: Buffer[B]): String = {
      val size = b.readInt(buf)
      b.readString(buf, size)
    }

    override def write[B](buf: B, t: String)(implicit b: Buffer[B]): Unit = {
//      val bytes = t.getBytes(StandardCharsets.UTF_8)
      b.writeInt(buf, t.length)
      b.writeString(buf, t)
    }
  }

  implicit val rwBigDecimal: ReadWrite[BigDecimal] = implicitly[ReadWrite[String]].imap(BigDecimal.apply)(_.toString())

  implicit def readSeq[T](implicit rwt: ReadWrite[T]): ReadWrite[Seq[T]] =
    new ReadWrite[Seq[T]] {
      override def read[B](buf: B)(implicit b: Buffer[B]): Seq[T] = {
        val len = b.readInt(buf)
        (0 until len).map { _ =>
          rwt.read(buf)
        }
      }

      override def write[B](buf: B, t: Seq[T])(implicit b: Buffer[B]): Unit = {
        b.writeInt(buf, t.length)
        t.foreach(x => rwt.write(buf, x))
      }
    }

  implicit def readTuple[X, Y](implicit rwx: ReadWrite[X], rwy: ReadWrite[Y]): ReadWrite[(X, Y)] =
    product2[(X, Y), X, Y](identity)(Tuple2.apply)

  implicit def readMap[K, V](implicit rwk: ReadWrite[K], rwv: ReadWrite[V]): ReadWrite[Map[K, V]] =
    implicitly[ReadWrite[Seq[(K, V)]]].imap(_.toMap)(_.toSeq)

  def product2[T, F1, F2](
      from: T => (F1, F2)
  )(to: (F1, F2) => T)(implicit rwf1: ReadWrite[F1], rwf2: ReadWrite[F2]): ReadWrite[T] = new ReadWrite[T] {
    override def read[B: Buffer](buf: B): T = to(rwf1.read(buf), rwf2.read(buf))

    override def write[B: Buffer](buf: B, t: T): Unit = {
      val (f1, f2) = from(t)
      rwf1.write(buf, f1)
      rwf2.write(buf, f2)
    }
  }

  def product3[T, F1, F2, F3](
      from: T => (F1, F2, F3)
  )(to: (F1, F2, F3) => T)(implicit rwf1: ReadWrite[F1], rwf2: ReadWrite[F2], rwf3: ReadWrite[F3]): ReadWrite[T] =
    new ReadWrite[T] {
      override def read[B: Buffer](buf: B): T = to(rwf1.read(buf), rwf2.read(buf), rwf3.read(buf))

      override def write[B: Buffer](buf: B, t: T): Unit = {
        val (f1, f2, f3) = from(t)
        rwf1.write(buf, f1)
        rwf2.write(buf, f2)
        rwf3.write(buf, f3)
      }
    }

  def product4[T, F1, F2, F3, F4](
      from: T => (F1, F2, F3, F4)
  )(
      to: (F1, F2, F3, F4) => T
  )(implicit rwf1: ReadWrite[F1], rwf2: ReadWrite[F2], rwf3: ReadWrite[F3], rwf4: ReadWrite[F4]): ReadWrite[T] =
    new ReadWrite[T] {
      override def read[B: Buffer](buf: B): T = to(rwf1.read(buf), rwf2.read(buf), rwf3.read(buf), rwf4.read(buf))

      override def write[B: Buffer](buf: B, t: T): Unit = {
        val (f1, f2, f3, f4) = from(t)
        rwf1.write(buf, f1)
        rwf2.write(buf, f2)
        rwf3.write(buf, f3)
        rwf4.write(buf, f4)
      }
    }

}
