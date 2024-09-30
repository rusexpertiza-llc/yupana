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

trait DivGuard[A, B, R] extends Guard2[A, B, R] {
  def div(a: A, b: B): R
}

object DivGuard {
  private lazy val instances: Map[(DataType, DataType), DivGuard[_, _, _]] = Map(
    (DataType[Byte], DataType[Byte]) -> intGuard[Byte],
    (DataType[Short], DataType[Short]) -> intGuard[Short],
    (DataType[Int], DataType[Int]) -> intGuard[Int],
    (DataType[Long], DataType[Long]) -> intGuard[Long],
    (DataType[Double], DataType[Double]) -> fracGuard[Double],
    (DataType[BigDecimal], DataType[BigDecimal]) -> fracGuard[BigDecimal],
    (DataType[BigDecimal], DataType[Double]) -> decimalDouble,
    (DataType[Double], DataType[BigDecimal]) -> doubleDecimal,
    (DataType[Currency], DataType[Int]) -> curIntGuard[Int],
    (DataType[Currency], DataType[Long]) -> curIntGuard[Long],
    (DataType[Currency], DataType[Double]) -> curDoubleGuard,
    (DataType[Currency], DataType[BigDecimal]) -> curDecimalGuard
  )

  def get(a: DataType, b: DataType): Option[DivGuard[_, _, _]] =
    instances.get((a, b))

  implicit def intGuard[N](implicit i: Integral[N], dt: DataType.Aux[N]): DivGuard[N, N, N] =
    create((a: N, d: N) => i.quot(a, d))
  implicit def fracGuard[N](implicit f: Fractional[N], dt: DataType.Aux[N]): DivGuard[N, N, N] =
    create((a: N, d: N) => f.div(a, d))

  implicit def curIntGuard[I](implicit i: Integral[I]): DivGuard[Currency, I, Currency] =
    create((a: Currency, b: I) => Currency(a.value / i.toLong(b)))

  implicit val decimalDouble: DivGuard[BigDecimal, Double, BigDecimal] = create((a: BigDecimal, b: Double) => a / b)
  implicit val doubleDecimal: DivGuard[Double, BigDecimal, BigDecimal] = create((a: Double, b: BigDecimal) => a / b)

  implicit val curDoubleGuard: DivGuard[Currency, Double, Currency] =
    create((c: Currency, x: Double) => Currency((c.value / x).toLong))
  implicit val curDecimalGuard: DivGuard[Currency, BigDecimal, Currency] =
    create((c: Currency, x: BigDecimal) => Currency((c.value / x).toLong))

  private def create[A, B, R](f: (A, B) => R)(implicit rdt: DataType.Aux[R]): DivGuard[A, B, R] =
    new DivGuard[A, B, R] {
      override def div(a: A, b: B): R = f(a, b)
      override val dataType: DataType.Aux[R] = rdt
    }
}
