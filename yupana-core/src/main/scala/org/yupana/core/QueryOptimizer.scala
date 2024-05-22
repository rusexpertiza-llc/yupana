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

package org.yupana.core

import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.core.utils.ExpressionUtils
import org.yupana.core.utils.ExpressionUtils.{ Id, Transformer }

object QueryOptimizer {

  def optimize(expressionCalculator: ConstantCalculator)(query: Query): Query = {
    query.copy(
      fields = query.fields.map(optimizeField(expressionCalculator, query.startTime, query.params)),
      filter = query.filter.map(optimizeCondition(expressionCalculator, query.startTime, query.params)),
      postFilter = query.postFilter.map(optimizeCondition(expressionCalculator, query.startTime, query.params)),
      groupBy = query.groupBy.map(e => optimizeExpr(expressionCalculator, query.startTime, query.params)(e))
    )
  }

  def optimizeCondition(expressionCalculator: ConstantCalculator, startTime: Time, params: Array[Any])(
      c: Condition
  ): Condition = {
    simplifyCondition(optimizeExpr(expressionCalculator, startTime, params)(c))
  }

  def optimizeField(expressionCalculator: ConstantCalculator, startTime: Time, params: Array[Any])(
      field: QueryField
  ): QueryField = {
    field.copy(expr = optimizeExpr(expressionCalculator, startTime, params)(field.expr))
  }

  def optimizeExpr[T](expressionCalculator: ConstantCalculator, startTime: Time, params: Array[Any])(
      expr: Expression[T]
  ): Expression[T] = {
    val transformer = new Transformer[Id] {
      override def apply[U](e: Expression[U]): Option[Expression[U]] = {
        if (e.kind == Const) Some(evaluateConstant(expressionCalculator, startTime, params)(e)) else None
      }
    }
    ExpressionUtils.transform(transformer)(expr)
  }

  def simplifyCondition(condition: Condition): Condition = {
    condition match {
      case AndExpr(cs) => or(optimizeAnd(List.empty, cs.toList))
      case OrExpr(cs)  => or(cs.flatMap(optimizeOr))
      case c           => c
    }
  }

  private def optimizeAnd(topAnd: List[Condition], cs: List[Condition]): List[Condition] = {
    cs match {
      case Nil                 => List(and(topAnd.reverse))
      case AndExpr(as) :: rest => optimizeAnd(topAnd, as.toList ::: rest)
      case OrExpr(os) :: rest  => os.flatMap(optimizeOr).flatMap(o => optimizeAnd(topAnd, o :: rest)).toList
      case x :: rest           => optimizeAnd(x :: topAnd, rest)
    }
  }

  private def optimizeOr(c: Condition): Seq[Condition] = {
    c match {
      case OrExpr(cs) => cs.flatMap(optimizeOr)
      case x          => Seq(simplifyCondition(x))
    }
  }

  private def and(conditions: Seq[Condition]): Condition = {
    if (conditions.exists(c => c == ConstantExpr(false) || c == FalseExpr)) {
      FalseExpr
    } else {
      val nonEmpty = conditions.filterNot(c => c == ConstantExpr(true) || c == TrueExpr)
      if (nonEmpty.size == 1) {
        nonEmpty.head
      } else if (nonEmpty.nonEmpty) {
        AndExpr(nonEmpty)
      } else {
        TrueExpr
      }
    }
  }

  private def or(conditions: Seq[Condition]): Condition = {
    if (conditions.exists(c => c == ConstantExpr(true) || c == TrueExpr)) {
      TrueExpr
    } else {
      val nonEmpty = conditions.filterNot(c => c == ConstantExpr(false) || c == FalseExpr)
      if (nonEmpty.size == 1) {
        nonEmpty.head
      } else if (nonEmpty.nonEmpty) {
        OrExpr(nonEmpty)
      } else {
        TrueExpr
      }
    }
  }

  private def evaluateConstant[T](
      expressionCalculator: ConstantCalculator,
      startTime: Time,
      params: Array[Any]
  )(e: Expression[T]): Expression[T] = {
    assert(e.kind == Const)
    val eval = expressionCalculator.evaluateConstant(e, Some(startTime), params)
    if (eval != null) {
      ConstantExpr(eval)(
        e.dataType
      )
    } else NullExpr[T](e.dataType)
  }
}
