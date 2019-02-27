package org.yupana.api.types

import org.joda.time.Period
import org.yupana.api.Time

trait BinaryOperation[T] extends Serializable {
  type U
  type Out

  def dataType: DataType.Aux[Out]
  val name: String
  def apply(x: T, y: U): Out

  override def toString: String = name
}

object BinaryOperation {
  type Aux[A, B, O] = BinaryOperation[A] { type U = B;  type Out = O }

  val PLUS = "plus"
  val MINUS = "minus"
  val MULTIPLY = "multiply"
  val DIVIDE = "divide"

  def plus[T](dt: DataType.Aux[T])(implicit num: Numeric[T]): BinaryOperation.Aux[T, T, T] = BinaryOperation(num.plus, "+", dt)
  def minus[T](dt: DataType.Aux[T])(implicit num: Numeric[T]): BinaryOperation.Aux[T, T, T] = BinaryOperation(num.minus, "-", dt)
  def multiply[T](dt: DataType.Aux[T])(implicit num: Numeric[T]): BinaryOperation.Aux[T, T, T] = BinaryOperation(num.times, "*", dt)
  def divideInt[T](dt: DataType.Aux[T])(implicit num: Integral[T]): BinaryOperation.Aux[T, T, T] = BinaryOperation(num.quot, "/", dt)
  def divideFrac[T](dt: DataType.Aux[T])(implicit num: Fractional[T]): BinaryOperation.Aux[T, T, T] = BinaryOperation(num.div, "/", dt)

  def timeMinusTime: BinaryOperation.Aux[Time, Time, Long] = BinaryOperation((a, b) => math.abs(a.millis - b.millis), "-", DataType[Long])
  def timeMinusPeriod: BinaryOperation.Aux[Time, Period, Time] = BinaryOperation(
    (t, p) => Time(t.toDateTime.minus(p).getMillis), "-", DataType[Time]
  )
  def timePlusPeriod: BinaryOperation.Aux[Time, Period, Time] = BinaryOperation(
    (t, p) => Time(t.toDateTime.plus(p).getMillis), "+", DataType[Time]
  )

  def periodPlusPeriod: BinaryOperation.Aux[Period, Period, Period] = BinaryOperation(_ plus _, "+", DataType[Period])

  def apply[A, B, O](fun: (A, B) => O, n: String, rt: DataType.Aux[O]): BinaryOperation.Aux[A, B, O] = new BinaryOperation[A] {
    override type U = B
    override type Out = O
    override val dataType: DataType.Aux[O] = rt
    override def apply(x: A, y: B): O = fun(x, y)
    override val name: String = n
  }

  def integralOperations[T : Integral](dt: DataType.Aux[T]): Map[(String, String), BinaryOperation[T]] = Map(
    entry(PLUS, dt, BinaryOperation.plus[T](dt)),
    entry(MINUS, dt, BinaryOperation.minus[T](dt)),
    entry(MULTIPLY, dt, BinaryOperation.multiply[T](dt)),
    entry(DIVIDE, dt, BinaryOperation.divideInt[T](dt))
  )

  def fractionalOperations[T : Fractional](dt: DataType.Aux[T]): Map[(String, String), BinaryOperation[T]] = Map(
    entry(PLUS, dt, BinaryOperation.plus[T](dt)),
    entry(MINUS, dt, BinaryOperation.minus[T](dt)),
    entry(MULTIPLY, dt, BinaryOperation.multiply[T](dt)),
    entry(DIVIDE, dt, BinaryOperation.divideFrac[T](dt))
  )

  val timeOperations: Map[(String, String), BinaryOperation[Time]] = Map(
    entry(MINUS, DataType[Time], BinaryOperation.timeMinusTime),
    entry(MINUS, DataType[Period], BinaryOperation.timeMinusPeriod),
    entry(PLUS, DataType[Period], BinaryOperation.timePlusPeriod)
  )

  val periodOperations: Map[(String, String), BinaryOperation[Period]] = Map(
    entry(PLUS, DataType[Period], BinaryOperation.periodPlusPeriod)
  )

  val stringOperations: Map[(String, String), BinaryOperation[String]] = Map(
    entry(PLUS, DataType[String], BinaryOperation[String, String, String](_ + _, "+", DataType[String]))
  )

  private def entry[T, U](name: String, argType: DataType.Aux[U], op: BinaryOperation.Aux[T, U, _]): ((String, String), BinaryOperation[T]) = {
    (name, argType.meta.sqlTypeName) -> op
  }
}
