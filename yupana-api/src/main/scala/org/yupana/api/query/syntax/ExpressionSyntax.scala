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
import org.yupana.api.schema.{ Dimension, ExternalLink, LinkField, Metric }
import org.yupana.api.types._

trait ExpressionSyntax {
  val time: TimeExpr.type = TimeExpr

//  def function[T, U](f: UnaryOperation.Aux[T, U], e: Expression[T]) = UnaryOperationExpr(f, e)
  def convert[T, U](tc: TypeConverter[T, U], e: Expression[T]) = TypeConvertExpr(tc, e)

  def tuple[T, U](e1: Expression[T], e2: Expression[U])(implicit rtt: DataType.Aux[T], rtu: DataType.Aux[U]) =
    TupleExpr(e1, e2)
  def array[T](es: Expression[T]*)(implicit dtt: DataType.Aux[T]) = ArrayExpr[T](Array(es: _*))
  def dimension[T](dim: Dimension[T]) = DimensionExpr(dim)
  def link(link: ExternalLink, fieldName: String): LinkExpr[String] =
    LinkExpr[String](link, LinkField[String](fieldName))
  def link[T](link: ExternalLink, field: LinkField.Aux[T]): LinkExpr[T] = LinkExpr[T](link, field)
  def metric[T](m: Metric.Aux[T]) = MetricExpr(m)
  def const[T](c: T)(implicit rt: DataType.Aux[T]): Expression[T] = ConstantExpr[T](c)

  def condition[T](condition: Condition, positive: Expression[T], negative: Expression[T]) =
    ConditionExpr(condition, positive, negative)

  def in[T](e: Expression[T], consts: Set[T]): Condition = InExpr(e, consts)
  def notIn[T](e: Expression[T], consts: Set[T]): Condition = NotInExpr(e, consts)

  def and(exprs: Condition*): Condition = AndExpr(Seq(exprs: _*))
  def or(exprs: Condition*): Condition = OrExpr(Seq(exprs: _*))

  def gt[T: Ordering](left: Expression[T], right: Expression[T]) =
    GtExpr(left, right)
  def lt[T: Ordering](left: Expression[T], right: Expression[T]) =
    LtExpr(left, right)
  def ge[T: Ordering](left: Expression[T], right: Expression[T]) =
    GeExpr(left, right)
  def le[T: Ordering](left: Expression[T], right: Expression[T]) =
    LeExpr(left, right)
  def equ[T](left: Expression[T], right: Expression[T]) =
    EqExpr(left, right)
  def neq[T](left: Expression[T], right: Expression[T]) =
    NeqExpr(left, right)
}

object ExpressionSyntax extends ExpressionSyntax
