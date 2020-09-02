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

import org.yupana.api.query.Expression.Condition
import org.yupana.api.query.{ AndExpr, Const, ConstantExpr, Expression, OrExpr, Query, QueryField, Transformer }

object QueryOptimizer {

  def optimize(query: Query): Query = {
    query.copy(
      fields = query.fields.map(optimizeField),
      filter = query.filter.map(optimizeCondition),
      postFilter = query.postFilter.map(optimizeCondition)
    )
  }

  private val transformer = new Transformer {
    override def apply[T](e: Expression[T]): Option[Expression[T]] = {
      if (e.kind == Const) Some(evaluateConstant(e)) else None
    }
  }

  def optimizeCondition(c: Condition): Condition = {
    simplifyCondition(optimizeExpr(c))
  }

  def optimizeField(field: QueryField): QueryField = {
    field.copy(expr = optimizeExpr(field.expr))
  }

  def optimizeExpr[T](expr: Expression[T]): Expression[T] = {
    expr.transform(transformer)
  }

  def simplifyCondition(condition: Condition): Condition = {
    condition match {
      case AndExpr(cs) => and(cs.flatMap(optimizeAnd))
      case OrExpr(cs)  => or(cs.flatMap(optimizeOr))
      case c           => c
    }
  }

  private def optimizeAnd(c: Condition): Seq[Condition] = {
    c match {
      case AndExpr(cs) => cs.flatMap(optimizeAnd)
      case x           => Seq(simplifyCondition(x))
    }
  }

  private def optimizeOr(c: Condition): Seq[Condition] = {
    c match {
      case OrExpr(cs) => cs.flatMap(optimizeOr)
      case x          => Seq(simplifyCondition(x))
    }
  }

  private def and(conditions: Seq[Condition]): Condition = {
    val nonEmpty = conditions.filterNot(_ == ConstantExpr(true))
    if (nonEmpty.size == 1) {
      nonEmpty.head
    } else if (nonEmpty.nonEmpty) {
      AndExpr(nonEmpty)
    } else {
      ConstantExpr(true)
    }
  }

  private def or(conditions: Seq[Condition]): Condition = {
    val nonEmpty = conditions.filterNot(_ == ConstantExpr(true))
    if (nonEmpty.size == 1) {
      nonEmpty.head
    } else if (nonEmpty.nonEmpty) {
      OrExpr(nonEmpty)
    } else {
      ConstantExpr(true)
    }
  }

  private def evaluateConstant[T](e: Expression[T]): Expression[T] = {
    assert(e.kind == Const)
    val eval = ExpressionCalculator.evaluateConstant(e)
    if (eval != null) {
      ConstantExpr(eval)(
        e.dataType
      )
    } else {
      throw new IllegalAccessException(s"Cannot evaluate constant expression $e")
    }

  }
}
