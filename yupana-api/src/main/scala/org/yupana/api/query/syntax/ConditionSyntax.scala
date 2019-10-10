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
import org.yupana.api.types.{ BinaryOperation, UnaryOperation }

trait ConditionSyntax {
  def in[T](e: Expression.Aux[T], consts: Set[T]) = InExpr(e, consts).aux
  def notIn[T](e: Expression.Aux[T], consts: Set[T]) = NotInExpr(e, consts).aux

  def and(exprs: Condition*) = AndExpr(Seq(exprs: _*)).aux
  def or(exprs: Condition*) = OrExpr(Seq(exprs: _*)).aux

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

object ConditionSyntax extends ConditionSyntax
