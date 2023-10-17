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

trait ReadWrite[T] {
  def read[B: Buffer](buf: B): T
  def write[B: Buffer](buf: B, t: T): Unit
}

object ReadWrite {
  implicit val rwInt: ReadWrite[Int] = new ReadWrite[Int] {
    override def read[B: Buffer](buf: B): Int = implicitly[Buffer[B]].readInt(buf)
    override def write[B: Buffer](buf: B, t: Int): Unit = implicitly[Buffer[B]].writeInt(buf, t)
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
