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

package org.yupana.api

import org.yupana.api.types.Num

case class Currency(value: Long) extends AnyVal {
  def toBigDecimal: BigDecimal = {
    BigDecimal.valueOf(value) / Currency.SUB
  }
}

object Currency {
  val SCALE: Int = 2
  val SUB: Int = Iterator.iterate(1)(_ * 10).drop(SCALE).next()

  def of(x: BigDecimal): Currency = {
    Currency((x * SUB).longValue)
  }

  implicit val ordering: Ordering[Currency] = Ordering.by(_.value)
  implicit val num: Num[Currency] = new Num[Currency] {
    override def negate(a: Currency): Currency = Currency(-a.value)
    override def abs(a: Currency): Currency = Currency(math.abs(a.value))
    override def toDouble(a: Currency): Double = a.value.toDouble / SUB
  }
}
