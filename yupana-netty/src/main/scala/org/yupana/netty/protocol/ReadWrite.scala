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

package org.yupana.netty.protocol

import io.netty.buffer.ByteBuf

import java.nio.charset.StandardCharsets

trait ReadWrite[T] {
  def read(buf: ByteBuf): T
  def write(buf: ByteBuf, t: T): Unit
}

object ReadWrite {
  implicit val rwInt: ReadWrite[Int] = new ReadWrite[Int] {
    override def read(buf: ByteBuf): Int = buf.readInt()
    override def write(buf: ByteBuf, t: Int): Unit = buf.writeInt(t)
  }

  implicit val rwString: ReadWrite[String] = new ReadWrite[String] {
    override def read(buf: ByteBuf): String = {
      val size = buf.readInt()
      buf.readCharSequence(size, StandardCharsets.UTF_8).toString
    }

    override def write(buf: ByteBuf, t: String): Unit = {
      val bytes = t.getBytes(StandardCharsets.UTF_8)
      buf.writeInt(bytes.length)
      buf.writeBytes(bytes)
    }
  }

  implicit def readMap[K, V](implicit rwk: ReadWrite[K], rwv: ReadWrite[V]): ReadWrite[Map[K, V]] =
    new ReadWrite[Map[K, V]] {
      override def read(buf: ByteBuf): Map[K, V] = {
        val len = buf.readInt()
        (0 until len).map { _ =>
          rwk.read(buf) -> rwv.read(buf)
        }.toMap
      }

      override def write(buf: ByteBuf, t: Map[K, V]): Unit = {
        buf.writeInt(t.size)
        t foreach {
          case (k, v) =>
            rwk.write(buf, k)
            rwv.write(buf, v)
        }
      }
    }
}
