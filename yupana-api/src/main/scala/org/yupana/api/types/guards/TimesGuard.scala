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

import org.yupana.api.Currency
import org.yupana.api.types.DataType

trait TimesGuard[N, M, R] extends Guard2[N, M, R] {
  def times(n: N, m: M): R
}

object TimesGuard {
  private lazy val instances: Map[(DataType, DataType), TimesGuard[_, _, _]] = Map(
    (DataType[Byte], DataType[Byte]) -> numGuard[Byte],
    (DataType[Short], DataType[Short]) -> numGuard[Short],
    (DataType[Int], DataType[Int]) -> numGuard[Int],
    (DataType[Long], DataType[Long]) -> numGuard[Long],
    (DataType[Double], DataType[Double]) -> numGuard[Double],
    (DataType[BigDecimal], DataType[BigDecimal]) -> numGuard[BigDecimal],
    (DataType[Currency], DataType[Int]) -> curIntGuard[Int],
    (DataType[Currency], DataType[Long]) -> curIntGuard[Long],
    (DataType[Currency], DataType[Double]) -> curDoubleGuard,
    (DataType[Currency], DataType[BigDecimal]) -> curDecimalGuard
  )

  def get(a: DataType, b: DataType): Option[TimesGuard[_, _, _]] =
    instances.get((a, b))

  implicit def numGuard[N](implicit num: Numeric[N], dt: DataType.Aux[N]): TimesGuard[N, N, N] =
    create((n: N, m: N) => num.times(n, m))

  implicit def curIntGuard[N](implicit num: Integral[N]): TimesGuard[Currency, N, Currency] =
    create((c: Currency, x: N) => Currency(num.toLong(x) * c.value))

  implicit val curDoubleGuard: TimesGuard[Currency, Double, Currency] =
    create((c: Currency, x: Double) => Currency((c.value * x).toLong))
  implicit val curDecimalGuard: TimesGuard[Currency, BigDecimal, Currency] =
    create((c: Currency, x: BigDecimal) => Currency((c.value * x).toLong))

  private def create[A, B, R](f: (A, B) => R)(implicit rdt: DataType.Aux[R]): TimesGuard[A, B, R] =
    new TimesGuard[A, B, R] {
      override def times(a: A, b: B): R = f(a, b)
      override val dataType: DataType.Aux[R] = rdt
    }
}
