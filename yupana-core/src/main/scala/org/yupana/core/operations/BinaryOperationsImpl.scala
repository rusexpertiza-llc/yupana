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

package org.yupana.core.operations

import org.joda.time.Period
import org.yupana.api.Time
import org.yupana.api.types.BinaryOperations

trait BinaryOperationsImpl extends BinaryOperations {
  // Number operations
  override def plus[N](a: N, b: N)(implicit numeric: Numeric[N]): N = numeric.plus(a, b)
  override def minus[N](a: N, b: N)(implicit numeric: Numeric[N]): N = numeric.minus(a, b)
  override def multiply[N](a: N, b: N)(implicit numeric: Numeric[N]): N = numeric.times(a, b)
  override def intDiv[N](a: N, b: N)(implicit integral: Integral[N]): N = integral.quot(a, b)
  override def fracDiv[N](a: N, b: N)(implicit fractional: Fractional[N]): N = fractional.div(a, b)

  // Time and Period operations
  override def minus(a: Time, b: Time): Long = math.abs(a.millis - b.millis)
  override def minus(t: Time, p: Period): Time = Time(t.toDateTime.minus(p).getMillis)
  override def plus(t: Time, p: Period): Time = Time(t.toDateTime.plus(p).getMillis)
  override def plus(a: Period, b: Period): Period = a plus b

  // String operations
  override def plus(a: String, b: String): String = a + b

  // Array operations
  override def contains[T](a: Array[T], t: T): Boolean = a contains t
  override def containsAll[T](a: Array[T], b: Array[T]): Boolean = b.forall(a.contains)
  override def containsAny[T](a: Array[T], b: Array[T]): Boolean = b.exists(a.contains)
  override def containsSame[T](a: Array[T], b: Array[T]): Boolean = a sameElements b

  // Ordering operations
  override def equ[T](a: T, b: T)(implicit ordering: Ordering[T]): Boolean = ordering.equiv(a, b)
  override def neq[T](a: T, b: T)(implicit ordering: Ordering[T]): Boolean = !ordering.equiv(a, b)
  override def gt[T](a: T, b: T)(implicit ordering: Ordering[T]): Boolean = ordering.gt(a, b)
  override def lt[T](a: T, b: T)(implicit ordering: Ordering[T]): Boolean = ordering.lt(a, b)
  override def ge[T](a: T, b: T)(implicit ordering: Ordering[T]): Boolean = ordering.gteq(a, b)
  override def le[T](a: T, b: T)(implicit ordering: Ordering[T]): Boolean = ordering.lteq(a, b)
}
