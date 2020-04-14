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
  val nullValue: T

  def read(a: Array[Byte]): T
  def write(t: T): Array[Byte]
}

object FixedStorable {

  def apply[T](implicit ev: FixedStorable[T]): FixedStorable[T] = ev

  implicit val longStorable: FixedStorable[Long] =
    of(jl.Long.BYTES, 0L, a => ByteBuffer.wrap(a).getLong, l => ByteBuffer.allocate(jl.Long.BYTES).putLong(l).array())

  implicit val intStorable: FixedStorable[Int] =
    of(
      jl.Integer.BYTES,
      0,
      a => ByteBuffer.wrap(a).getInt,
      i => ByteBuffer.allocate(jl.Integer.BYTES).putInt(i).array()
    )

  implicit val doubleStorable: FixedStorable[Double] =
    of(
      jl.Double.BYTES,
      0d,
      a => ByteBuffer.wrap(a).getDouble,
      d => ByteBuffer.allocate(jl.Double.BYTES).putDouble(d).array()
    )

  implicit val timeStorable: FixedStorable[Time] =
    of(longStorable.size, Time(0), a => Time(longStorable.read(a)), t => longStorable.write(t.millis))

  def of[T](s: Int, n: T, r: Array[Byte] => T, w: T => Array[Byte]): FixedStorable[T] = new FixedStorable[T] {
    override val size: Int = s
    override val nullValue: T = n

    override def read(a: Array[Byte]): T = r(a)
    override def write(t: T): Array[Byte] = w(t)
  }
}
