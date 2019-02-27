package org.yupana.api.query.syntax

import org.yupana.api.query._
import org.yupana.api.schema.{ExternalLink, Measure}
import org.yupana.api.types._

trait ExpressionSyntax {
  def time: TimeExpr.type = TimeExpr

  def function[T, U](f: UnaryOperation.Aux[T, U], e: Expression.Aux[T]) = FunctionExpr(f, e)
  def function[T, U](f: TypeConverter[T, U], e: Expression.Aux[T]) = FunctionExpr(f, e)

  def tuple[T, U](e1: Expression.Aux[T], e2: Expression.Aux[U])(implicit rtt: DataType.Aux[T], rtu: DataType.Aux[U]) = TupleExpr(e1, e2)
  def dimension(dimName: String) = new DimensionExpr(dimName)
  def link(link: ExternalLink, fieldName: String) = new LinkExpr(link, fieldName)
  def measure[T](field: Measure.Aux[T]) = MeasureExpr(field)
  def const[T](c: T)(implicit rt: DataType.Aux[T]): Expression.Aux[T] = ConstantExpr[T](c)

  def aggregate[T](a: Aggregation[T], f: Measure.Aux[T]): Expression.Aux[a.Out] = aggregate(a, measure(f))
  def aggregate[T](a: Aggregation[T], e: Expression.Aux[T]): Expression.Aux[a.Out] = AggregateExpr(a, e)

  def bi[T, U, O](op: BinaryOperation.Aux[T, U, O], a: Expression.Aux[T], b: Expression.Aux[U]) = BinaryOperationExpr(op, a, b)

  def windowFunction[T](wf: WindowOperation[T], e: Expression.Aux[T]) = WindowFunctionExpr(wf, e)

  def condition[T](condition: Condition, positive: Expression.Aux[T], negative: Expression.Aux[T]) = ConditionExpr(condition, positive, negative)
}

object ExpressionSyntax extends ExpressionSyntax
