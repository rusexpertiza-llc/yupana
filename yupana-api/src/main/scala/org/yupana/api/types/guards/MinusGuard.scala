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

package org.yupana.api.types.guards

import org.threeten.extra.PeriodDuration
import org.yupana.api.{ Currency, Time }
import org.yupana.api.types.DataType

sealed trait MinusGuard[A, B, R] extends Guard2[A, B, R] {
  def minus(a: A, b: B): R
}

object MinusGuard {
  private lazy val instances: Map[(DataType, DataType), MinusGuard[_, _, _]] = Map(
    entry[Byte, Byte, Byte],
    entry[Short, Short, Short],
    entry[Int, Int, Int],
    entry[Long, Long, Long],
    entry[Double, Double, Double],
    entry[BigDecimal, BigDecimal, BigDecimal],
    entry[BigDecimal, Double, BigDecimal],
    entry[Double, BigDecimal, BigDecimal],
    entry[Currency, Currency, Currency],
    entry[Time, PeriodDuration, Time],
    entry[Time, Time, Long]
  )

  private def entry[A, B, R](
      implicit a: DataType.Aux[A],
      b: DataType.Aux[B],
      g: MinusGuard[A, B, R]
  ): ((DataType.Aux[A], DataType.Aux[B]), MinusGuard[A, B, R]) = (a, b) -> g

  def get(a: DataType, b: DataType): Option[MinusGuard[_, _, _]] =
    instances.get((a, b))

  implicit def numMinus[N: Numeric: DataType.Aux]: MinusGuard[N, N, N] =
    create((a: N, b: N) => implicitly[Numeric[N]].minus(a, b))

  implicit val decimalDouble: MinusGuard[BigDecimal, Double, BigDecimal] = create((a: BigDecimal, b: Double) => a - b)
  implicit val doubleDecimal: MinusGuard[Double, BigDecimal, BigDecimal] = create((a: Double, b: BigDecimal) => a - b)

  implicit val currency: MinusGuard[Currency, Currency, Currency] =
    create((a: Currency, b: Currency) => Currency(a.value - b.value))

  implicit val timePeriod: MinusGuard[Time, PeriodDuration, Time] =
    create((a: Time, b: PeriodDuration) => Time(a.toDateTime.minus(b)))

  implicit val time: MinusGuard[Time, Time, Long] = create((a: Time, b: Time) => math.abs(a.millis - b.millis))

  private def create[A, B, R](f: (A, B) => R)(implicit rdt: DataType.Aux[R]): MinusGuard[A, B, R] =
    new MinusGuard[A, B, R] {
      override def minus(a: A, b: B): R = f(a, b)
      override val dataType: DataType.Aux[R] = rdt
    }
}
