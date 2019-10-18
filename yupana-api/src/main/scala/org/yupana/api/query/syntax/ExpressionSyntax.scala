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

package org.yupana.api.query.syntax

import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.schema.{ Dimension, ExternalLink, Metric }
import org.yupana.api.types._

trait ExpressionSyntax {
  val time: TimeExpr.type = TimeExpr

  def function[T, U](f: UnaryOperation.Aux[T, U], e: Expression.Aux[T]) = UnaryOperationExpr(f, e)
  def convert[T, U](tc: TypeConverter[T, U], e: Expression.Aux[T]) = TypeConvertExpr(tc, e)

  def tuple[T, U](e1: Expression.Aux[T], e2: Expression.Aux[U])(implicit rtt: DataType.Aux[T], rtu: DataType.Aux[U]) =
    TupleExpr(e1, e2)
  def array[T](es: Expression.Aux[T]*)(implicit dtt: DataType.Aux[T]) = ArrayExpr[T](Array(es: _*))
  def dimension(dim: Dimension) = new DimensionExpr(dim)
  def link(link: ExternalLink, fieldName: String) = new LinkExpr(link, fieldName)
  def metric[T](m: Metric.Aux[T]) = MetricExpr(m)
  def const[T](c: T)(implicit rt: DataType.Aux[T]): Expression.Aux[T] = ConstantExpr[T](c)

  def aggregate[T](a: Aggregation[T], f: Metric.Aux[T]): Expression.Aux[a.Out] = aggregate(a, metric(f))
  def aggregate[T](a: Aggregation[T], e: Expression.Aux[T]): Expression.Aux[a.Out] = AggregateExpr(a, e)

  def bi[T, U, O](op: BinaryOperation.Aux[T, U, O], a: Expression.Aux[T], b: Expression.Aux[U]) =
    BinaryOperationExpr(op, a, b)

  def windowFunction[T](wf: WindowOperation[T], e: Expression.Aux[T]) = WindowFunctionExpr(wf, e)

  def condition[T](condition: Condition, positive: Expression.Aux[T], negative: Expression.Aux[T]) =
    ConditionExpr(condition, positive, negative)

  def in[T](e: Expression.Aux[T], consts: Set[T]): Condition = InExpr(e, consts).aux
  def notIn[T](e: Expression.Aux[T], consts: Set[T]): Condition = NotInExpr(e, consts).aux

  def and(exprs: Condition*): Condition = AndExpr(Seq(exprs: _*)).aux
  def or(exprs: Condition*): Condition = OrExpr(Seq(exprs: _*)).aux

  def gt[T: Ordering](left: Expression.Aux[T], right: Expression.Aux[T]) =
    BinaryOperationExpr(BinaryOperation.gt, left, right)
  def lt[T: Ordering](left: Expression.Aux[T], right: Expression.Aux[T]) =
    BinaryOperationExpr(BinaryOperation.lt, left, right)
  def ge[T: Ordering](left: Expression.Aux[T], right: Expression.Aux[T]) =
    BinaryOperationExpr(BinaryOperation.ge, left, right)
  def le[T: Ordering](left: Expression.Aux[T], right: Expression.Aux[T]) =
    BinaryOperationExpr(BinaryOperation.le, left, right)
  def equ[T: Ordering](left: Expression.Aux[T], right: Expression.Aux[T]) =
    BinaryOperationExpr(BinaryOperation.equ, left, right)
  def neq[T: Ordering](left: Expression.Aux[T], right: Expression.Aux[T]) =
    BinaryOperationExpr(BinaryOperation.neq, left, right)

  def isNull[T](e: Expression.Aux[T]) = UnaryOperationExpr(UnaryOperation.isNull[T], e)
  def isNotNull[T](e: Expression.Aux[T]) = UnaryOperationExpr(UnaryOperation.isNotNull[T], e)
}

object ExpressionSyntax extends ExpressionSyntax
