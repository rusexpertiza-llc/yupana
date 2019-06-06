package org.yupana.api.types

import org.yupana.api.Time

/**
  * Aggregation definition
  *
  * Aggregation performs in three steps: map-reduce-postMap. For example, we'd like to count all letters in a bunch of strings
  * and return value as a string. Then the steps will be:
  *
  * 1. map: {{{ (s: String) => BigInt(s.length) }}}
  * 2. reduce {{{ (a: BigInt, b: BigInt) = a + b }}}
  * 3. postMap {{{ (i: BigInt) => i.toString }}}
  *
  * @tparam T input type of aggregation
  */
trait Aggregation[T] extends Serializable {
  /** Type after first map operation */
  type Interim
  /** Output type */
  type Out
  /** This aggregation name */
  val name: String

  /**
    * Map input value of type `T` to `Interim` type
    * @param t value to be mapped
    * @param a instance of aggregations implementation
    * @return mapped value
    */
  def map(t: T)(implicit a: Aggregations): Interim

  /**
    * Reduces two mapped values into one value
    * @param x first value
    * @param y second value
    * @param a instance of aggregations implementation
    * @return reduced value
    */
  def reduce(x: Interim, y: Interim)(implicit a: Aggregations): Interim

  /**
    * Converts reduced value of type [[Interim]] to output type
    * @param x value to be converted
    * @param a instance of aggregations implementation
    * @return converted value
    */
  def postMap(x: Interim)(implicit a: Aggregations): Out

  /** Output data type */
  val dataType: DataType.Aux[Out]
}

object Aggregation {
  type Aux[T, U, V] = Aggregation[T] { type Interim = U; type Out = V }

  val SUM = "sum"
  val MAX = "max"
  val MIN = "min"
  val COUNT = "count"
  val DISTINCT_COUNT = "distinct_count"

  type Reducer[T] = (T, T) => T

  def sum[T](implicit num: Numeric[T], dt: DataType.Aux[T]): Aggregation.Aux[T, T, T] = create(SUM, _.sum, dt)

  def min[T](implicit ord: Ordering[T], dt: DataType.Aux[T]): Aggregation.Aux[T, T, T] = create(MIN, _.min, dt)

  def max[T](implicit ord: Ordering[T], dt: DataType.Aux[T]): Aggregation.Aux[T, T, T] = create(MAX, _.max, dt)

  def count[T]: Aggregation.Aux[T, Long, Long] = create(COUNT, _.count, DataType[Long])

  def distinctCount[T]: Aggregation.Aux[T, Set[T], Int] = create(DISTINCT_COUNT, _.distinctCount, DataType[Int])

  def create[T, U, V](n: String, f: Aggregations => AggregationImpl[T, U, V], dt: DataType.Aux[V]): Aux[T, U, V] = new Aggregation[T] {
    override type Out = V
    override type Interim = U
    override val name: String = n
    override def map(t: T)(implicit a: Aggregations): U = f(a).map(t)
    override def reduce(x: U, y: U)(implicit a: Aggregations): U = f(a).reduce(x, y)
    override def postMap(x: U)(implicit a: Aggregations): V = f(a).postMap(x)

    override val dataType: DataType.Aux[Out] = dt
  }

  lazy val stringAggregations: Map[String, Aggregation[String]] = Map(
    MAX -> Aggregation.max[String](Ordering[String], DataType[String]),
    MIN -> Aggregation.min[String](Ordering[String], DataType[String]),
    COUNT -> Aggregation.count[String],
    DISTINCT_COUNT -> Aggregation.distinctCount[String]
  )

  lazy val timeAggregations: Map[String, Aggregation[Time]] = Map(
    MAX -> Aggregation.max[Time](Ordering[Time], DataType[Time]),
    MIN -> Aggregation.min[Time](Ordering[Time], DataType[Time]),
    COUNT -> Aggregation.count[Time],
    DISTINCT_COUNT -> Aggregation.distinctCount[Time]
  )

  def intAggregations[T](dt: DataType.Aux[T])(implicit i: Integral[T]): Map[String, Aggregation[T]]  = Map(
    SUM -> Aggregation.sum[T](i, dt),
    MAX -> Aggregation.max[T](i, dt),
    MIN -> Aggregation.min[T](i, dt),
    COUNT -> Aggregation.count[T],
    DISTINCT_COUNT -> Aggregation.distinctCount[T]
  )

  def fracAggregations[T](dt: DataType.Aux[T])(implicit f: Fractional[T]): Map[String, Aggregation[T]]  = Map(
    SUM -> Aggregation.sum[T](f, dt),
    MAX -> Aggregation.max[T](f, dt),
    MIN -> Aggregation.min[T](f, dt),
    COUNT -> Aggregation.count[T],
    DISTINCT_COUNT -> Aggregation.distinctCount[T]
  )
}

class AggregationImpl[T, I, O](val map: T => I, val reduce: (I, I) => I, val postMap: I => O)

trait Aggregations {
  def sum[T: Numeric]: AggregationImpl[T, T, T]
  def min[T: Ordering]: AggregationImpl[T, T, T]
  def max[T: Ordering]: AggregationImpl[T, T, T]
  def count[T]: AggregationImpl[T, Long, Long]
  def distinctCount[T]: AggregationImpl[T, Set[T], Int]
}
