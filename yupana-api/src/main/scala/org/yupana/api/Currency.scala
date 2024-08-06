package org.yupana.api

case class Currency(value: Long) extends AnyVal

object Currency {
  val SUB: Int = 100

  def of(x: BigDecimal): Currency = {
    Currency((x * SUB).longValue)
  }

  implicit val numeric: Integral[Currency] = new Integral[Currency] {
    override def quot(x: Currency, y: Currency): Currency = ???

    override def rem(x: Currency, y: Currency): Currency = ???

    override def times(x: Currency, y: Currency): Currency = ???

    override def plus(x: Currency, y: Currency): Currency = Currency(x.value + y.value)

    override def minus(x: Currency, y: Currency): Currency = Currency(x.value - y.value)

    override def negate(x: Currency): Currency = Currency(-x.value)

    override def fromInt(x: Int): Currency = Currency(x)

    override def parseString(str: String): Option[Currency] = ???

    override def toInt(x: Currency): Int = x.value.toInt

    override def toLong(x: Currency): Long = x.value

    override def toFloat(x: Currency): Float = x.value.toFloat

    override def toDouble(x: Currency): Double = x.value.toDouble

    override def compare(x: Currency, y: Currency): Int = x.value compare y.value
  }
}
