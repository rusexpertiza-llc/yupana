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

  def tuple[T, U](e1: Expression[T], e2: Expression[U]): TupleExpr[T, U] = TupleExpr(e1, e2)
  def tupleValue[T, U](v1: T, v2: U)(implicit dtt: DataType.Aux[T], dtu: DataType.Aux[U]): TupleValueExpr[T, U] =
    TupleValueExpr(ConstantExpr(v1), ConstantExpr(v2))
  def array[T](es: Expression[T]*)(implicit dtt: DataType.Aux[T]) = ArrayExpr[T](Seq(es: _*))
  def dimension[T](dim: Dimension.Aux[T]) = DimensionExpr(dim)
  def link(link: ExternalLink, fieldName: String): LinkExpr[String] =
    LinkExpr[String](link, LinkField[String](fieldName))
  def link[T](link: ExternalLink, field: LinkField.Aux[T]): LinkExpr[T] = LinkExpr[T](link, field)
  def metric[T](m: Metric.Aux[T]) = MetricExpr(m)
  def const[T](c: T)(implicit rt: DataType.Aux[T]): ConstantExpr[T] = ConstantExpr[T](c)
  def param[T](id: Int)(implicit dt: DataType.Aux[T]): Expression[T] = PlaceholderExpr(id, dt)

  def condition[T](condition: Condition, positive: Expression[T], negative: Expression[T]) =
    ConditionExpr(condition, positive, negative)

  def in[T](e: Expression[T], consts: Set[T]): InExpr[T] = InExpr(e, consts.map(x => ConstantExpr(x)(e.dataType)))
  def notIn[T](e: Expression[T], consts: Set[T]): NotInExpr[T] =
    NotInExpr(e, consts.map(x => ConstantExpr(x)(e.dataType)))

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
