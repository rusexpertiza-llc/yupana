package org.yupana.api.types

import org.joda.time.DateTimeFieldType
import org.yupana.api.Time

/**
  * Unary function definition
  * @tparam T type to thus function be applied
  */
trait UnaryOperation[T] extends Serializable {
  /** Output type */
  type Out

  /** This function name */
  val name: String

  /** Output data type */
  def dataType: DataType.Aux[Out]

  /**
    * Applies this function on passed value
    * @param t optional value to apply function
    * @param unaryOperations unary functions implementation instance
    * @return optional calculated value
    */
  def apply(t: Option[T])(implicit unaryOperations: UnaryOperations): Option[Out]

  override def toString: String = name
}

object UnaryOperation {

  val LENGTH = "length"
  val STEM = "stem"
  val SPLIT = "split"

  val IS_NULL = "is_null"
  val IS_NOT_NULL = "is_not_null"
  val NOT = "not"

  val MINUS = "-"
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

  val ARRAY_TO_STRING = "array_to_string"

  type Aux[T, U] = UnaryOperation[T] { type Out = U }

  def abs[T : Numeric](implicit dt: DataType.Aux[T]): UnaryOperation.Aux[T, T] = create(_.abs[T], ABS, dt)
  def minus[T : Numeric](implicit dt: DataType.Aux[T]): UnaryOperation.Aux[T, T] = create(_.unaryMinus[T], MINUS, dt)

  def isNull[T]: UnaryOperation.Aux[T, Boolean] = create(_.isNull[T], IS_NULL, DataType[Boolean])
  def isNotNull[T]: UnaryOperation.Aux[T, Boolean] = create(_.isNotNull[T], IS_NOT_NULL, DataType[Boolean])
  val not: UnaryOperation.Aux[Boolean, Boolean] = create(_.not, NOT, DataType[Boolean])

  val extractYear: UnaryOperation.Aux[Time, Int] = create(_.extractYear, EXTRACT_YEAR, DataType[Int])
  val extractMonth: UnaryOperation.Aux[Time, Int] = create(_.extractMonth, EXTRACT_MONTH, DataType[Int])
  val extractDay: UnaryOperation.Aux[Time, Int] = create(_.extractDay, EXTRACT_DAY, DataType[Int])
  val extractHour: UnaryOperation.Aux[Time, Int] = create(_.extractHour, EXTRACT_HOUR, DataType[Int])
  val extractMinute: UnaryOperation.Aux[Time, Int] = create(_.extractMinute, EXTRACT_MINUTE, DataType[Int])
  val extractSecond: UnaryOperation.Aux[Time, Int] = create(_.extractSecond, EXTRACT_SECOND, DataType[Int])

  def trunc(interval: DateTimeFieldType): UnaryOperation.Aux[Time, Time] = create(_.trunc(interval), TRUNC, DataType[Time])
  val truncYear: UnaryOperation.Aux[Time, Time] = create(_.truncYear, TRUNC_YEAR, DataType[Time])
  val truncMonth: UnaryOperation.Aux[Time, Time] = create(_.truncMonth, TRUNC_MONTH, DataType[Time])
  val truncDay: UnaryOperation.Aux[Time, Time] = create(_.truncDay, TRUNC_DAY, DataType[Time])
  val truncHour: UnaryOperation.Aux[Time, Time] = create(_.truncHour, TRUNC_HOUR, DataType[Time])
  val truncMinute: UnaryOperation.Aux[Time, Time] = create(_.truncMinute, TRUNC_MINUTE, DataType[Time])
  val truncSecond: UnaryOperation.Aux[Time, Time] = create(_.truncSecond, TRUNC_SECOND, DataType[Time])
  val truncWeek: UnaryOperation.Aux[Time, Time] = create(_.truncWeek, TRUNC_WEEK, DataType[Time])

  val length: UnaryOperation.Aux[String, Int] = create(_.stringLength, LENGTH, DataType[Int])
  val stem: UnaryOperation.Aux[String, Array[String]] = create(_.stemString, STEM, DataType[Array[String]])
  val splitString: UnaryOperation.Aux[String, Array[String]] = create(_.splitString, SPLIT, DataType[Array[String]])

  def arrayToString[T]: UnaryOperation.Aux[Array[T], String] = create(_.arrayToString[T], ARRAY_TO_STRING, DataType[String])
  def arrayLength[T]: UnaryOperation.Aux[Array[T], Int] = create(_.arrayLength[T], LENGTH, DataType[Int])
  val stemArray: UnaryOperation.Aux[Array[String], Array[String]] = create(_.stemArray, STEM, DataType[Array[String]])

  def create[T, U](fun: UnaryOperations => Option[T] => Option[U], n: String, dt: DataType.Aux[U]): UnaryOperation.Aux[T, U] = new UnaryOperation[T] {
    override type Out = U
    override val name: String = n
    override def dataType: DataType.Aux[U] = dt
    override def apply(t: Option[T])(implicit unaryOperations: UnaryOperations): Option[U] = fun(unaryOperations)(t)
  }

  val stringOperations: Map[String, UnaryOperation[String]] = Map(
    LENGTH -> length,
    STEM -> stem,
    SPLIT -> splitString
  )

  val boolOperations: Map[String, UnaryOperation[Boolean]] = Map(
    NOT -> not
  )

  def numericOperations[T](dt: DataType.Aux[T])(implicit n: Numeric[T]): Map[String, UnaryOperation[T]] = Map(
    ABS -> abs(n, dt),
    MINUS -> minus(n, dt)
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
    TRUNC_WEEK -> truncWeek,
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

  def arrayOperations[T]: Map[String, UnaryOperation[Array[T]]] = Map(
    ARRAY_TO_STRING -> arrayToString[T],
    LENGTH -> arrayLength[T]
  )

  val stringArrayOperations: Map[String, UnaryOperation[Array[String]]] = Map(
    STEM -> stemArray
  )

  def tupleOperations[A, B](aOps: Map[String, UnaryOperation[A]],
                            bOps: Map[String, UnaryOperation[B]]
                           ): Map[String, UnaryOperation[(A, B)]] = {
    val commonOps = aOps.keySet intersect bOps.keySet
    commonOps.map { c =>
      val ao = aOps(c)
      val bo = bOps(c)
      c -> new UnaryOperation[(A, B)] {
        override type Out = (ao.Out, bo.Out)
        override val name: String = ao.name

        override def dataType: DataType.Aux[(ao.Out, bo.Out)] = DataType.tupleDt(ao.dataType, bo.dataType)

        override def apply(t: Option[(A, B)])(implicit unaryOperations: UnaryOperations): Option[(ao.Out, bo.Out)] = {
          for {
            a <- ao(t.map(_._1))
            b <- bo(t.map(_._2))
          } yield (a, b)
        }
      }
    }.toMap
  }
}

trait UnaryOperations {
  def unaryMinus[N: Numeric](n: Option[N]): Option[N]
  def abs[N: Numeric](n: Option[N]): Option[N]

  def isNull[T](t: Option[T]): Option[Boolean]
  def isNotNull[T](t: Option[T]): Option[Boolean]
  def not(x: Option[Boolean]): Option[Boolean]

  def extractYear(t: Option[Time]): Option[Int]
  def extractMonth(t: Option[Time]): Option[Int]
  def extractDay(t: Option[Time]): Option[Int]
  def extractHour(t: Option[Time]): Option[Int]
  def extractMinute(t: Option[Time]): Option[Int]
  def extractSecond(t: Option[Time]): Option[Int]

  def trunc(fieldType: DateTimeFieldType)(time: Option[Time]): Option[Time]
  def truncYear(t: Option[Time]): Option[Time]
  def truncMonth(t: Option[Time]): Option[Time]
  def truncWeek(t: Option[Time]): Option[Time]
  def truncDay(t: Option[Time]): Option[Time]
  def truncHour(t: Option[Time]): Option[Time]
  def truncMinute(t: Option[Time]): Option[Time]
  def truncSecond(t: Option[Time]): Option[Time]

  def stringLength(s: Option[String]): Option[Int]
  def stemString(s: Option[String]): Option[Array[String]]
  def splitString(s: Option[String]): Option[Array[String]]

  def arrayToString[T](a: Option[Array[T]]): Option[String]
  def arrayLength[T](a: Option[Array[T]]): Option[Int]
  def stemArray(a: Option[Array[String]]): Option[Array[String]]
}

trait Operations {
  def apply[T](dt: DataType.Aux[T]): TypeOperations[T]
}
