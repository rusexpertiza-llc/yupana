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

  implicit def readMap[K, V](implicit rwk: ReadWrite[K], rwv: ReadWrite[V]): ReadWrite[Map[K, V]] =
    new ReadWrite[Map[K, V]] {
      override def read[B](buf: B)(implicit b: Buffer[B]): Map[K, V] = {
        val len = b.readInt(buf)
        (0 until len).map { _ =>
          rwk.read[B](buf) -> rwv.read(buf)
        }.toMap
      }

      override def write[B](buf: B, t: Map[K, V])(implicit b: Buffer[B]): Unit = {
        b.writeInt(buf, t.size)
        t foreach {
          case (k, v) =>
            rwk.write(buf, k)
            rwv.write(buf, v)
        }
      }
    }
}
