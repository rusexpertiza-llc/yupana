package org.yupana.core.operations

import org.yupana.api.types.{AggregationImpl, Aggregations}

trait AggregationsImpl extends Aggregations {
  override def sum[T](implicit n: Numeric[T]): AggregationImpl[T, T, T] = new AggregationImpl(identity, n.plus, identity)

  override def min[T](implicit o: Ordering[T]): AggregationImpl[T, T, T] = new AggregationImpl(identity, o.min, identity)

  override def max[T](implicit o: Ordering[T]): AggregationImpl[T, T, T] = new AggregationImpl(identity, o.max, identity)

  override def count[T]: AggregationImpl[T, Long, Long] = new AggregationImpl(_ => 1L, _ + _, identity)

  override def distinctCount[T]: AggregationImpl[T, Set[T], Int] = new AggregationImpl(x => Set(x), _ union _, _.size)
}
