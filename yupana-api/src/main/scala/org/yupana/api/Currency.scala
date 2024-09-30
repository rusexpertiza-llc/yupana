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

case class Currency(value: Long) extends AnyVal {
  def toBigDecimal: BigDecimal = {
    BigDecimal.valueOf(value) / Currency.SUB
  }
}

object Currency {
  val SUB: Int = 100

  def of(x: BigDecimal): Currency = {
    Currency((x * SUB).longValue)
  }

  implicit val numeric: Fractional[Currency] = new Fractional[Currency] {
    override def times(x: Currency, y: Currency): Currency = ???
    override def div(x: Currency, y: Currency): Currency = ???

    override def plus(x: Currency, y: Currency): Currency = Currency(x.value + y.value)

    override def minus(x: Currency, y: Currency): Currency = Currency(x.value - y.value)

    override def negate(x: Currency): Currency = Currency(-x.value)

    override def fromInt(x: Int): Currency = Currency(x)

    override def parseString(str: String): Option[Currency] = Fractional[BigDecimal].parseString(str).map(of)

    override def toInt(x: Currency): Int = x.value.toInt

    override def toLong(x: Currency): Long = x.value

    override def toFloat(x: Currency): Float = x.value.toFloat

    override def toDouble(x: Currency): Double = x.value.toDouble

    override def compare(x: Currency, y: Currency): Int = x.value compare y.value

  }
}
