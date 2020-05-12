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

import org.yupana.api.query.{ Const, ConstantExpr, Expression, Query, QueryField }

object QueryOptimizer {

  def optimize(query: Query): Query = {
    query.copy(
      fields = query.fields.map(optimizeField),
      filter = query.filter.map(optimizeExpr),
      postFilter = query.postFilter.map(optimizeExpr)
    )
  }

  def optimizeField(field: QueryField): QueryField = {
    field.copy(expr = optimizeExpr(field.expr.aux))
  }

  def optimizeExpr[T](expr: Expression.Aux[T]): Expression.Aux[T] = {
    expr.transform { case e if e.kind == Const => evaluateConstant(e.aux).aux }
  }

  private def evaluateConstant[T](e: Expression.Aux[T]): Expression.Aux[T] = {
    assert(e.kind == Const)
    ConstantExpr(
      ExpressionCalculator
        .evaluateConstant(e)
        .getOrElse(throw new IllegalAccessException(s"Cannot evaluate constant expression $e"))
    )(
      e.dataType
    )
  }
}
