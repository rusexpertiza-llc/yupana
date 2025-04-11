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

/**
  * Type class for data serialization from / to the ByteReaderWriter
  *
  * @tparam T type to be serialized
  */
trait ReadWrite[T] extends Read[T] with Write[T] { self =>

  /**
    * Creates a ReadWrite instance for type A from the instance for type T
    * @param to wraps instance of T into A
    * @param from extracts instance of T from A
    * @tparam A a new type
    */
  def imap[A](to: T => A)(from: A => T): ReadWrite[A] = new ReadWrite[A] {
    override def read[B: ByteReaderWriter](buf: B): A = to(self.read(buf))
    override def write[B: ByteReaderWriter](buf: B, t: A): Unit = self.write(buf, from(t))
  }
}

object ReadWrite {
  implicit def apply[T](implicit r: Read[T], w: Write[T]): ReadWrite[T] = new ReadWrite[T] {
    override def write[B: ByteReaderWriter](buf: B, t: T): Unit = w.write(buf, t)
    override def read[B: ByteReaderWriter](buf: B): T = r.read(buf)
  }

  val empty: ReadWrite[Unit] = new ReadWrite[Unit] {
    override def read[B: ByteReaderWriter](buf: B): Unit = ()
    override def write[B: ByteReaderWriter](buf: B, t: Unit): Unit = ()
  }

  def product2[T, F1, F2](to: (F1, F2) => T)(
      from: T => (F1, F2)
  )(implicit rwf1: ReadWrite[F1], rwf2: ReadWrite[F2]): ReadWrite[T] = new ReadWrite[T] {
    override def read[B: ByteReaderWriter](buf: B): T = Read.product2(to).read(buf)
    override def write[B: ByteReaderWriter](buf: B, t: T): Unit = Write.product2(from).write(buf, t)
  }

  def product3[T, F1, F2, F3](to: (F1, F2, F3) => T)(
      from: T => (F1, F2, F3)
  )(implicit rwf1: ReadWrite[F1], rwf2: ReadWrite[F2], rwf3: ReadWrite[F3]): ReadWrite[T] =
    new ReadWrite[T] {
      override def read[B: ByteReaderWriter](buf: B): T = Read.product3(to).read(buf)
      override def write[B: ByteReaderWriter](buf: B, t: T): Unit = Write.product3(from).write(buf, t)
    }

  def product4[T, F1, F2, F3, F4](to: (F1, F2, F3, F4) => T)(
      from: T => (F1, F2, F3, F4)
  )(implicit rwf1: ReadWrite[F1], rwf2: ReadWrite[F2], rwf3: ReadWrite[F3], rwf4: ReadWrite[F4]): ReadWrite[T] =
    new ReadWrite[T] {
      override def read[B: ByteReaderWriter](buf: B): T = Read.product4(to).read(buf)
      override def write[B: ByteReaderWriter](buf: B, t: T): Unit = Write.product4(from).write(buf, t)
    }
}
