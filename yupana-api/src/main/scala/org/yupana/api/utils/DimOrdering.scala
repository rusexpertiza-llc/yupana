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

package org.yupana.api.utils

trait DimOrdering[T] {
  def gt(a: T, b: T): Boolean
  def lt(a: T, b: T): Boolean

  def gte(a: T, b: T): Boolean = !lt(a, b)
  def lte(a: T, b: T): Boolean = !lt(b, a)

  def min(a: T, b: T): T
  def max(a: T, b: T): T
}

object DimOrdering {

  implicit val byteDimOrdering: DimOrdering[Byte] =
    fromCmp((a, b) => java.lang.Byte.compare((a + Byte.MinValue).toByte, (b + Byte.MinValue).toByte))
  implicit val shortDimOrdering: DimOrdering[Short] =
    fromCmp((a, b) => java.lang.Short.compare((a + Short.MinValue).toShort, (b + Short.MinValue).toShort))
  implicit val intDimOrdering: DimOrdering[Int] = fromCmp(java.lang.Integer.compareUnsigned)
  implicit val longDimOrdering: DimOrdering[Long] = fromCmp(java.lang.Long.compareUnsigned)
  implicit val stringDimOrdering: DimOrdering[String] = fromCmp(Ordering[String].compare)

  def fromCmp[T](cmp: (T, T) => Int): DimOrdering[T] = new DimOrdering[T] {
    override def lt(x: T, y: T): Boolean = cmp(x, y) < 0
    override def gt(x: T, y: T): Boolean = cmp(x, y) > 0

    override def max(a: T, b: T): T = if (gte(a, b)) a else b
    override def min(a: T, b: T): T = if (lte(a, b)) a else b
  }
}
