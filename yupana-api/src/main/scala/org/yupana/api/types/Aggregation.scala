package org.yupana.api.types

import org.yupana.api.Time

trait Aggregation[T] extends Serializable {
  type Interim
  type Out
  val name: String
  val map: T => Interim
  val reduce: Aggregation.Reducer[Interim]
  val postMap: Interim => Out
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

  def sum[T](dt: DataType.Aux[T])(implicit num: Numeric[T]): Aggregation.Aux[T, T, T] = Aggregation[T](SUM, num.plus, dt)
  def sum[T](implicit num: Numeric[T], dt: DataType.Aux[T]): Aggregation.Aux[T, T, T] = Aggregation[T](SUM, num.plus, dt)

  def min[T](dt: DataType.Aux[T])(implicit ord: Ordering[T]): Aggregation.Aux[T, T, T] = Aggregation[T](MIN, ord.min, dt)
  def min[T](implicit ord: Ordering[T], dt: DataType.Aux[T]): Aggregation.Aux[T, T, T] = Aggregation[T](MIN, ord.min, dt)

  def max[T](dt: DataType.Aux[T])(implicit ord: Ordering[T]): Aggregation.Aux[T, T, T] = Aggregation[T](MAX, ord.max, dt)
  def max[T](implicit ord: Ordering[T], dt: DataType.Aux[T]): Aggregation.Aux[T, T, T] = Aggregation[T](MAX, ord.max, dt)

  def count[T]: Aggregation.Aux[T, Long, Long] = Aggregation[T, Long](COUNT, _ => 1L, _ + _, DataType[Long])

  def distinctCount[T]: Aggregation.Aux[T, Set[T], Int] = Aggregation[T, Set[T], Int](
    DISTINCT_COUNT, x => Set(x), _ union _, _.size, DataType[Int]
  )

  def apply[T, U, V](n: String, m: T => U, r: Reducer[U], pm: U => V, dt: DataType.Aux[V]): Aux[T, U, V] = new Aggregation[T] {
    override type Out = V
    override type Interim = U
    override val name: String = n
    override val map: T => U = m
    override val reduce: Reducer[U] = r
    override val postMap: U => V = pm
    override val dataType: DataType.Aux[Out] = dt
  }

  def apply[T, U](name: String, map: T => U, reducer: Reducer[U], dataType: DataType.Aux[U]): Aggregation.Aux[T, U, U] =
    Aggregation(name, map, reducer, identity, dataType)

  def apply[T](name: String, r: Reducer[T], dt: DataType.Aux[T]): Aux[T, T, T] = Aggregation(name, identity, r, identity, dt)

  lazy val stringAggregations: Map[String, Aggregation[String]] = Map(
    MAX -> Aggregation.max[String](DataType[String]),
    MIN -> Aggregation.min[String](DataType[String]),
    COUNT -> Aggregation.count[String],
    DISTINCT_COUNT -> Aggregation.distinctCount[String]
  )

  lazy val timeAggregations: Map[String, Aggregation[Time]] = Map(
    MAX -> Aggregation.max[Time](DataType[Time]),
    MIN -> Aggregation.min[Time](DataType[Time]),
    COUNT -> Aggregation.count[Time],
    DISTINCT_COUNT -> Aggregation.distinctCount[Time]
  )

  def intAggregations[T : Integral](dt: DataType.Aux[T]): Map[String, Aggregation[T]]  = Map(
    SUM -> Aggregation.sum[T](dt),
    MAX -> Aggregation.max[T](dt),
    MIN -> Aggregation.min[T](dt),
    COUNT -> Aggregation.count[T],
    DISTINCT_COUNT -> Aggregation.distinctCount[T]
  )

  def fracAggregations[T : Fractional](dt: DataType.Aux[T]): Map[String, Aggregation[T]]  = Map(
    SUM -> Aggregation.sum[T](dt),
    MAX -> Aggregation.max[T](dt),
    MIN -> Aggregation.min[T](dt),
    COUNT -> Aggregation.count[T],
    DISTINCT_COUNT -> Aggregation.distinctCount[T]
  )
}
