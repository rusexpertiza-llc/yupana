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

trait PlusGuard[A, B, R] extends Guard2[A, B, R] {
  def plus(a: A, b: B): R
}

object PlusGuard {
  private lazy val instances: Map[(DataType, DataType), PlusGuard[_, _, _]] = Map(
    (DataType[Byte], DataType[Byte]) -> numPlus[Byte],
    (DataType[Short], DataType[Short]) -> numPlus[Short],
    (DataType[Int], DataType[Int]) -> numPlus[Int],
    (DataType[Long], DataType[Long]) -> numPlus[Long],
    (DataType[Double], DataType[Double]) -> numPlus[Double],
    (DataType[BigDecimal], DataType[BigDecimal]) -> numPlus[BigDecimal],
    (DataType[Double], DataType[BigDecimal]) -> doubleDecimal,
    (DataType[BigDecimal], DataType[Double]) -> decimalDouble,
    (DataType[Double], DataType[Long]) -> doubleLong,
    (DataType[Long], DataType[Double]) -> longDouble,
    (DataType[Currency], DataType[Currency]) -> currency,
    (DataType[String], DataType[String]) -> string,
    (DataType[Time], DataType[PeriodDuration]) -> timePeriod,
    (DataType[PeriodDuration], DataType[PeriodDuration]) -> period
  )

  def get(a: DataType, b: DataType): Option[PlusGuard[_, _, _]] =
    instances.get((a, b))

  implicit def numPlus[N: Numeric: DataType.Aux]: PlusGuard[N, N, N] =
    create((a: N, b: N) => implicitly[Numeric[N]].plus(a, b))

  implicit val doubleDecimal: PlusGuard[Double, BigDecimal, BigDecimal] = create((a: Double, b: BigDecimal) => a + b)
  implicit val decimalDouble: PlusGuard[BigDecimal, Double, BigDecimal] = create((a: BigDecimal, b: Double) => a + b)

  implicit val doubleLong: PlusGuard[Double, Long, Double] = create((a: Double, b: Long) => a + b)
  implicit val longDouble: PlusGuard[Long, Double, Double] = create((a: Long, b: Double) => a + b)

  implicit val currency: PlusGuard[Currency, Currency, Currency] =
    create((a: Currency, b: Currency) => Currency(a.value + b.value))

  implicit val string: PlusGuard[String, String, String] = create((a: String, b: String) => a + b)
  implicit val timePeriod: PlusGuard[Time, PeriodDuration, Time] =
    create((a: Time, b: PeriodDuration) => Time(a.toDateTime.plus(b)))
  implicit val period: PlusGuard[PeriodDuration, PeriodDuration, PeriodDuration] =
    create((a: PeriodDuration, b: PeriodDuration) => a.plus(b))

  private def create[A, B, R](f: (A, B) => R)(implicit rdt: DataType.Aux[R]): PlusGuard[A, B, R] =
    new PlusGuard[A, B, R] {
      override def plus(a: A, b: B): R = f(a, b)
      override val dataType: DataType.Aux[R] = rdt
    }
}
