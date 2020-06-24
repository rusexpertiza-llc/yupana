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

import org.yupana.core.model.InternalRow
import org.yupana.api.query._
import org.yupana.core.operations.Operations

object ExpressionCalculator {

  implicit private val operations: Operations = Operations

  def evaluateConstant(expr: Expression): expr.Out = {
    assert(expr.kind == Const)
    eval(expr, null, null)
  }

  def evaluateExpression(
      expr: Expression,
      queryContext: QueryContext,
      internalRow: InternalRow,
      tryEval: Boolean = true
  ): expr.Out = {
    if (queryContext != null && queryContext.exprsIndex.contains(expr)
      && !internalRow.isEmpty(queryContext, expr) && tryEval) {
      eval(expr, queryContext, internalRow)
    } else {
      null.asInstanceOf[expr.Out]
    }
  }

  private def eval(expr: Expression, queryContext: QueryContext, internalRow: InternalRow): expr.Out = {

    val res = expr match {
      case ConstantExpr(x) => x//.asInstanceOf[expr.Out]

      case TimeExpr         => null
      case DimensionExpr(_) => null
      case MetricExpr(_)    => null
      case LinkExpr(_, _)   => null

      case ConditionExpr(condition, positive, negative) =>
        val x = evaluateExpression(condition, queryContext, internalRow)
        if (x) {
          evaluateExpression(positive, queryContext, internalRow)
        } else {
          evaluateExpression(negative, queryContext, internalRow)
        }

      case UnaryOperationExpr(f, e) =>
        f(evaluateExpression(e, queryContext, internalRow))

      case BinaryOperationExpr(f, a, b) =>
        f(evaluateExpression(a, queryContext, internalRow), evaluateExpression(b, queryContext, internalRow))

      case TypeConvertExpr(tc, e) =>
        tc.direct(evaluateExpression(e, queryContext, internalRow))

      case AggregateExpr(_, e) =>
        evaluateExpression(e, queryContext, internalRow)

      case WindowFunctionExpr(_, e) =>
        evaluateExpression(e, queryContext, internalRow)

      case InExpr(e, vs) =>
        vs contains evaluateExpression(e, queryContext, internalRow)

      case NotInExpr(e, vs) =>
        !vs.contains(evaluateExpression(e, queryContext, internalRow))

      case AndExpr(cs) =>
        val executed = cs.map(c => evaluateExpression(c, queryContext, internalRow))
        executed.reduce((a, b) => a && b)

      case OrExpr(cs) =>
        val executed = cs.map(c => evaluateExpression(c, queryContext, internalRow))
        executed.reduce((a, b) => a || b)

      case TupleExpr(e1, e2) =>
        (evaluateExpression(e1, queryContext, internalRow), evaluateExpression(e2, queryContext, internalRow))

      case ae @ ArrayExpr(es) =>
        val values: Array[ae.elementDataType.T] =
          Array.ofDim[ae.elementDataType.T](es.length)(ae.elementDataType.classTag)
        var success = true
        var i = 0

        while (i < es.length && success) {
          evaluateExpression(es(i), queryContext, internalRow) match {
            case Some(v) => values(i) = v.asInstanceOf[ae.elementDataType.T]
            case None    => success = false
          }

          i += 1
        }

        if (success) values else null

      case x => throw new IllegalArgumentException(s"Unsupported expression $x")
    }

    // I cannot find a better solution to ensure compiler that concrete expr type Out is the same with expr.Out
    res.asInstanceOf[expr.Out]
  }
}
