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

trait LowPriorityDivGuard {
  implicit def nDoubleGuard[N](implicit n: Numeric[N]): DivGuard[N, Double, Double] =
    create((x: N, d: Double) => n.toDouble(x) / d)
  implicit def doubleNGuard[N](implicit n: Numeric[N]): DivGuard[Double, N, Double] =
    create((d: Double, x: N) => d / n.toDouble(x))

  protected def create[A, B, R](f: (A, B) => R)(implicit rdt: DataType.Aux[R]): DivGuard[A, B, R] =
    new DivGuard[A, B, R] {
      override def div(a: A, b: B): R = f(a, b)
      override val dataType: DataType.Aux[R] = rdt
    }
}

object DivGuard extends LowPriorityDivGuard {
  private lazy val instances: Map[DataType, List[(DataType, DivGuard[_, _, _])]] = List(
    entry[Byte, Byte, Byte],
    entry[Short, Short, Short],
    entry[Int, Int, Int],
    entry[Long, Long, Long],
    entry[Double, Double, Double],
    entry[BigDecimal, BigDecimal, BigDecimal],
    entry[BigDecimal, Double, BigDecimal],
    entry[Double, BigDecimal, BigDecimal],
    entry[Long, Double, Double],
    entry[Double, Long, Double],
    entry[Int, Double, Double],
    entry[Double, Int, Double],
    entry[Currency, Long, Currency],
    entry[Currency, BigDecimal, Currency],
    entry[Currency, Int, Currency],
    entry[Currency, Double, Currency],
    entry[Currency, Currency, Double]
  ).groupMap(_._1)(x => (x._2, x._3))

  private def entry[A, B, R](
      implicit a: DataType.Aux[A],
      b: DataType.Aux[B],
      g: DivGuard[A, B, R]
  ): (DataType, DataType, DivGuard[A, B, R]) = (a, b, g)

  def get(a: DataType): List[(DataType, DivGuard[_, _, _])] =
    instances.getOrElse(a, Nil)

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

  implicit val curCurGuard: DivGuard[Currency, Currency, Double] =
    create((a: Currency, b: Currency) => a.value.toDouble / b.value)
}
