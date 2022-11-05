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

import java.nio.ByteBuffer
import java.{ lang => jl }

import org.yupana.api.Time

import scala.annotation.implicitNotFound

@implicitNotFound("No member of type class FixedStorable for class ${T} is found")
trait FixedStorable[T] extends Serializable {
  val size: Int

  def read(a: Array[Byte]): T
  def read(bb: ByteBuffer): T
  def write(t: T): Array[Byte]
}

object FixedStorable {

  def apply[T](implicit ev: FixedStorable[T]): FixedStorable[T] = ev

  implicit val longStorable: FixedStorable[Long] = of(jl.Long.BYTES, _.getLong, _.putLong)
  implicit val intStorable: FixedStorable[Int] = of(jl.Integer.BYTES, _.getInt, _.putInt)
  implicit val doubleStorable: FixedStorable[Double] = of(jl.Double.BYTES, _.getDouble, _.putDouble)
  implicit val shortStorable: FixedStorable[Short] = of(jl.Short.BYTES, _.getShort, _.putShort)
  implicit val byteStorable: FixedStorable[Byte] = of(jl.Byte.BYTES, _.get, _.put)
  implicit val timeStorable: FixedStorable[Time] = wrap(longStorable, (l: Long) => new Time(l), _.millis)
  implicit def tupleStorable[T, U](
      implicit tStrable: FixedStorable[T],
      uStorable: FixedStorable[U]
  ): FixedStorable[(T, U)] = {
    of(
      tStrable.size + uStorable.size,
      bb => (tStrable.read(bb), uStorable.read(bb)),
      bb => d => bb.put(tStrable.write(d._1)).put(uStorable.write(d._2))
    )
  }

  def of[T](s: Int, r: ByteBuffer => T, w: ByteBuffer => T => ByteBuffer): FixedStorable[T] = {
    new FixedStorable[T] {
      override val size: Int = s

      override def read(bb: ByteBuffer): T = r(bb)
      override def read(a: Array[Byte]): T = read(ByteBuffer.wrap(a))
      override def write(t: T): Array[Byte] = w(ByteBuffer.allocate(size))(t).array()
    }
  }

  def wrap[T, U](storable: FixedStorable[T], from: T => U, to: U => T): FixedStorable[U] = new FixedStorable[U] {
    override val size: Int = storable.size

    override def read(a: Array[Byte]): U = from(storable.read(a))
    override def read(bb: ByteBuffer): U = from(storable.read(bb))
    override def write(t: U): Array[Byte] = storable.write(to(t))
  }
}
