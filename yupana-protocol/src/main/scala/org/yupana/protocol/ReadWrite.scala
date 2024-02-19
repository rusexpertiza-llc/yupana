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

/**
  * Type class for data serialization from / to the [[Buffer]]
  *
  * @tparam T type to be serialized
  */
trait ReadWrite[T] { self =>

  /** Deserialize data from buffer */
  def read[B: Buffer](buf: B): T

  /** Serialize data into buffer */
  def write[B: Buffer](buf: B, t: T): Unit

  /**
    * Creates a ReadWrite instance for type A from the instance for type T
    * @param to wraps instance of T into A
    * @param from extracts instance of T from A
    * @tparam A a new type
    */
  def imap[A](to: T => A)(from: A => T): ReadWrite[A] = new ReadWrite[A] {
    override def read[B: Buffer](buf: B): A = to(self.read(buf))
    override def write[B: Buffer](buf: B, t: A): Unit = self.write(buf, from(t))
  }
}

object ReadWrite {
  def apply[T](implicit ev: ReadWrite[T]): ReadWrite[T] = ev

  implicit val rwBytes: ReadWrite[Array[Byte]] = new ReadWrite[Array[Byte]] {
    override def read[B](buf: B)(implicit b: Buffer[B]): Array[Byte] = {
      val length = b.readInt(buf)
      val result = new Array[Byte](length)
      b.read(buf, result)
      result
    }

    override def write[B](buf: B, t: Array[Byte])(implicit b: Buffer[B]): Unit = {
      b.writeInt(buf, t.length)
      b.write(buf, t)
    }
  }

  val empty: ReadWrite[Unit] = new ReadWrite[Unit] {
    override def read[B: Buffer](buf: B): Unit = ()
    override def write[B: Buffer](buf: B, t: Unit): Unit = ()
  }

  implicit val rwByte: ReadWrite[Byte] = new ReadWrite[Byte] {
    override def read[B: Buffer](buf: B): Byte = implicitly[Buffer[B]].readByte(buf)
    override def write[B: Buffer](buf: B, t: Byte): Unit = implicitly[Buffer[B]].writeByte(buf, t)
  }

  implicit val rwInt: ReadWrite[Int] = new ReadWrite[Int] {
    override def read[B: Buffer](buf: B): Int = implicitly[Buffer[B]].readInt(buf)
    override def write[B: Buffer](buf: B, t: Int): Unit = implicitly[Buffer[B]].writeInt(buf, t)
  }

  implicit val rwLong: ReadWrite[Long] = new ReadWrite[Long] {
    override def read[B](buf: B)(implicit b: Buffer[B]): Long = b.readLong(buf)
    override def write[B](buf: B, t: Long)(implicit b: Buffer[B]): Unit = b.writeLong(buf, t)
  }

  implicit val rwString: ReadWrite[String] = new ReadWrite[String] {
    override def read[B](buf: B)(implicit b: Buffer[B]): String = b.readString(buf)
    override def write[B](buf: B, t: String)(implicit b: Buffer[B]): Unit = b.writeString(buf, t)
  }

  implicit val rwBigDecimal: ReadWrite[BigDecimal] = implicitly[ReadWrite[String]].imap(BigDecimal.apply)(_.toString())

  implicit def rwOption[T](implicit rwt: ReadWrite[T]): ReadWrite[Option[T]] = new ReadWrite[Option[T]] {
    override def read[B](buf: B)(implicit b: Buffer[B]): Option[T] = {
      if (b.readByte(buf) == 1) Some(rwt.read(buf)) else None
    }

    override def write[B](buf: B, t: Option[T])(implicit b: Buffer[B]): Unit = {
      t match {
        case Some(t) =>
          b.writeByte(buf, 1)
          rwt.write(buf, t)
        case None => b.writeByte(buf, 0)
      }
    }
  }

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
    product2[(X, Y), X, Y](Tuple2.apply)(identity)

  implicit def readMap[K, V](implicit rwk: ReadWrite[K], rwv: ReadWrite[V]): ReadWrite[Map[K, V]] =
    implicitly[ReadWrite[Seq[(K, V)]]].imap(_.toMap)(_.toSeq)

  def product2[T, F1, F2](to: (F1, F2) => T)(
      from: T => (F1, F2)
  )(implicit rwf1: ReadWrite[F1], rwf2: ReadWrite[F2]): ReadWrite[T] = new ReadWrite[T] {
    override def read[B: Buffer](buf: B): T = to(rwf1.read(buf), rwf2.read(buf))

    override def write[B: Buffer](buf: B, t: T): Unit = {
      val (f1, f2) = from(t)
      rwf1.write(buf, f1)
      rwf2.write(buf, f2)
    }
  }

  def product3[T, F1, F2, F3](to: (F1, F2, F3) => T)(
      from: T => (F1, F2, F3)
  )(implicit rwf1: ReadWrite[F1], rwf2: ReadWrite[F2], rwf3: ReadWrite[F3]): ReadWrite[T] =
    new ReadWrite[T] {
      override def read[B: Buffer](buf: B): T = to(rwf1.read(buf), rwf2.read(buf), rwf3.read(buf))

      override def write[B: Buffer](buf: B, t: T): Unit = {
        val (f1, f2, f3) = from(t)
        rwf1.write(buf, f1)
        rwf2.write(buf, f2)
        rwf3.write(buf, f3)
      }
    }

  def product4[T, F1, F2, F3, F4](to: (F1, F2, F3, F4) => T)(
      from: T => (F1, F2, F3, F4)
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
