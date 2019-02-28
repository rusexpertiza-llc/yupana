package org.yupana.api.types

import org.joda.time.DateTimeFieldType
import org.yupana.api.Time

trait UnaryOperation[T] extends Serializable {
  type Out
  val name: String
  def dataType: DataType.Aux[Out]

  def apply(t: T): Out

  override def toString: String = name
}

object UnaryOperation {

  val LENGTH = "length"

  val ABS = "abs"

  val YEAR = "year"
  val MONTH = "month"
  val DAY = "day"
  val HOUR = "hour"
  val MINUTE = "minute"
  val SECOND = "second"

  val TRUNC = "trunc"
  val TRUNC_YEAR = "trunc_year"
  val TRUNC_MONTH = "trunc_month"
  val TRUNC_DAY = "trunc_day"
  val TRUNC_HOUR = "trunc_hour"
  val TRUNC_MINUTE = "trunc_minute"
  val TRUNC_SECOND = "trunc_second"
  val TRUNC_WEEK = "trunc_week"

  val EXTRACT_YEAR = "extract_year"
  val EXTRACT_MONTH = "extract_month"
  val EXTRACT_DAY = "extract_day"
  val EXTRACT_HOUR = "extract_hour"
  val EXTRACT_MINUTE = "extract_minute"
  val EXTRACT_SECOND = "extract_second"

  type Aux[T, U] = UnaryOperation[T] { type Out = U }

  def abs[T](dt: DataType.Aux[T])(implicit numeric: Numeric[T]): UnaryOperation.Aux[T, T] = UnaryOperation(numeric.abs, ABS, dt)

  val extractYear: UnaryOperation.Aux[Time, Int] = UnaryOperation(_.toLocalDateTime.getYear, EXTRACT_YEAR, DataType[Int])
  val extractMonth: UnaryOperation.Aux[Time, Int] = UnaryOperation(_.toLocalDateTime.getMonthOfYear, EXTRACT_MONTH, DataType[Int])
  val extractDay: UnaryOperation.Aux[Time, Int] = UnaryOperation(_.toLocalDateTime.getDayOfMonth, EXTRACT_DAY, DataType[Int])
  val extractHour: UnaryOperation.Aux[Time, Int] = UnaryOperation(_.toLocalDateTime.getHourOfDay, EXTRACT_HOUR, DataType[Int])
  val extractMinute: UnaryOperation.Aux[Time, Int] = UnaryOperation(_.toLocalDateTime.getMinuteOfHour, EXTRACT_MINUTE, DataType[Int])
  val extractSecond: UnaryOperation.Aux[Time, Int] = UnaryOperation(_.toLocalDateTime.getSecondOfMinute, EXTRACT_SECOND, DataType[Int])

  def trunc(interval: DateTimeFieldType): UnaryOperation.Aux[Time, Time] = UnaryOperation(truncateTime(_, interval), TRUNC, DataType[Time])
  val truncYear: UnaryOperation.Aux[Time, Time] = UnaryOperation(truncateTime(_, DateTimeFieldType.year()), TRUNC_YEAR, DataType[Time])
  val truncMonth: UnaryOperation.Aux[Time, Time] = UnaryOperation(truncateTime(_, DateTimeFieldType.monthOfYear()), TRUNC_MONTH, DataType[Time])
  val truncDay: UnaryOperation.Aux[Time, Time] = UnaryOperation(truncateTime(_, DateTimeFieldType.dayOfYear()), TRUNC_DAY, DataType[Time])
  val truncHour: UnaryOperation.Aux[Time, Time] = UnaryOperation(truncateTime(_, DateTimeFieldType.hourOfDay()), TRUNC_HOUR, DataType[Time])
  val truncMinute: UnaryOperation.Aux[Time, Time] = UnaryOperation(truncateTime(_, DateTimeFieldType.minuteOfHour()), TRUNC_MINUTE, DataType[Time])
  val truncSecond: UnaryOperation.Aux[Time, Time] = UnaryOperation(truncateTime(_, DateTimeFieldType.secondOfDay()), TRUNC_SECOND, DataType[Time])
  val truncWeek: UnaryOperation.Aux[Time, Time] = UnaryOperation(truncateTime(_, DateTimeFieldType.weekOfWeekyear()), TRUNC_WEEK, DataType[Time])

  private def truncateTime(time: Time, interval: DateTimeFieldType): Time = {
    Time(time.toDateTime.property(interval).roundFloorCopy().getMillis)
  }

  def apply[T, U](fun: T => U, n: String, dt: DataType.Aux[U]): UnaryOperation.Aux[T, U] = new UnaryOperation[T] {
    override type Out = U
    override val name: String = n
    override def dataType: DataType.Aux[U] = dt
    override def apply(t: T): U = fun(t)
  }

  val length: UnaryOperation.Aux[String, Int] = UnaryOperation(_.length, LENGTH, DataType[Int])

  def stringOperations: Map[String, UnaryOperation[String]] = Map(
    LENGTH -> length
  )

  def numericOperations[T : Numeric](dt: DataType.Aux[T]): Map[String, UnaryOperation[T]] = Map(
    ABS -> abs(dt)
  )

  def timeOperations: Map[String, UnaryOperation[Time]] = Map(
    YEAR -> truncYear,
    MONTH -> truncMonth,
    DAY -> truncDay,
    HOUR -> truncHour,
    MINUTE -> truncMinute,
    SECOND -> truncSecond,
    TRUNC_YEAR -> truncYear,
    TRUNC_MONTH -> truncMonth,
    TRUNC_DAY -> truncDay,
    TRUNC_HOUR -> truncHour,
    TRUNC_MINUTE -> truncMinute,
    TRUNC_SECOND -> truncSecond,
    EXTRACT_YEAR -> extractYear,
    EXTRACT_MONTH -> extractMonth,
    EXTRACT_DAY -> extractDay,
    EXTRACT_HOUR -> extractHour,
    EXTRACT_MINUTE -> extractMinute,
    EXTRACT_SECOND -> extractSecond
  )
}
