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

import org.yupana.api.Currency

trait Num[N] {
  def plus(a: N, b: N): N
  def minus(a: N, b: N): N

  def times[B](a: N, b: B)(implicit tg: Num.TimesGuard[N, B]): N
  def div[D](a: N, b: D)(implicit dg: Num.DivGuard[N, D]): N

  def negate(a: N): N
  def abs(a: N): N

  def toDouble(a: N): Double
}

object Num {
  trait TimesGuard[N, B] {
    def times(n: N, b: B): N
  }
  trait DivGuard[N, D] {
    def div(n: N, d: D): N
  }

  implicit def timesGuardNumeric[N](implicit numeric: Numeric[N]): TimesGuard[N, N] = (n: N, b: N) =>
    numeric.times(n, b)

  implicit def divGuardInt[N](implicit integral: Integral[N]): DivGuard[N, N] = (n: N, d: N) => integral.quot(n, d)
  implicit def divGuardFrac[N](implicit integral: Fractional[N]): DivGuard[N, N] = (n: N, d: N) => integral.div(n, d)
  implicit val currDivLong: DivGuard[Currency, Long] = (n: Currency, d: Long) => Currency(n.value / d)
  implicit val currDivInt: DivGuard[Currency, Int] = (n: Currency, d: Int) => Currency(n.value / d)

  implicit def fromNumeric[N](implicit n: Numeric[N]): Num[N] = new Num[N] {
    override def plus(a: N, b: N): N = n.plus(a, b)

    override def minus(a: N, b: N): N = n.minus(a, b)

    override def times[B](a: N, b: B)(implicit tg: TimesGuard[N, B]): N = tg.times(a, b)

    override def div[D](a: N, b: D)(implicit dg: DivGuard[N, D]): N = dg.div(a, b)

    override def negate(a: N): N = n.negate(a)

    override def abs(a: N): N = n.abs(a)

    override def toDouble(a: N): Double = n.toDouble(a)
  }
}
