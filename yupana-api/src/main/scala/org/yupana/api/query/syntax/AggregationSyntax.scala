package org.yupana.api.query.syntax

import org.yupana.api.query.{AggregateExpr, Expression}
import org.yupana.api.types.{Aggregation, DataType}

trait AggregationSyntax {
  def sum[T](e: Expression.Aux[T])(implicit n: Numeric[T], dt: DataType.Aux[T]) = AggregateExpr(Aggregation.sum[T], e)
  def min[T](e: Expression.Aux[T])(implicit ord: Ordering[T], dt: DataType.Aux[T]) = AggregateExpr(Aggregation.min[T], e)
  def max[T](e: Expression.Aux[T])(implicit ord: Ordering[T], dt: DataType.Aux[T]) = AggregateExpr(Aggregation.max[T], e)
  def count[T](e: Expression.Aux[T]) = AggregateExpr(Aggregation.count[T], e)
  def distinctCount[T](e: Expression.Aux[T]) = AggregateExpr(Aggregation.distinctCount[T], e)
}

object AggregationSyntax extends AggregationSyntax
