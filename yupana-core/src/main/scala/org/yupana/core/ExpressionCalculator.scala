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

  def evaluateConstant(expr: Expression): expr.Out = {
    assert(expr.kind == Const)
    eval(expr, null, null)
  }

  def preEvaluated(expr: Expression, queryContext: QueryContext, internalRow: InternalRow): expr.Out = {
    expr match {
      case ConstantExpr(v) => v.asInstanceOf[expr.Out]
      case _               => internalRow.get[expr.Out](queryContext, expr)
    }
  }

  def evaluateExpression(
      expr: Expression,
      queryContext: QueryContext,
      internalRow: InternalRow
  ): expr.Out = {
    val res = if (queryContext != null) {
      val idx = queryContext.exprsIndex.getOrElse(expr, -1)
      if (idx >= 0) {
        internalRow.get[expr.Out](idx)
      } else {
        null.asInstanceOf[expr.Out]
      }
    } else {
      null.asInstanceOf[expr.Out]
    }

    if (res == null) {
      eval(expr, queryContext, internalRow)
    } else {
      res
    }
  }

  private def eval(expr: Expression, queryContext: QueryContext, internalRow: InternalRow): expr.Out = {

    val res = expr match {
      case ConstantExpr(x) => x //.asInstanceOf[expr.Out]

      case TimeExpr           => null
      case DimensionExpr(_)   => null
      case DimensionIdExpr(_) => null
      case MetricExpr(_)      => null
      case LinkExpr(_, _)     => null

      case ConditionExpr(condition, positive, negative) =>
        val x = evaluateExpression(condition, queryContext, internalRow)
        if (x) {
          evaluateExpression(positive, queryContext, internalRow)
        } else {
          evaluateExpression(negative, queryContext, internalRow)
        }

      case TruncYearExpr(e) =>
        evaluateUnary(queryContext, internalRow)(e, Operations.truncYear)

      case TruncMonthExpr(e) =>
        evaluateUnary(queryContext, internalRow)(e, Operations.truncMonth)

      case TruncDayExpr(e) =>
        evaluateUnary(queryContext, internalRow)(e, Operations.truncDay)

      case TruncWeekExpr(e) =>
        evaluateUnary(queryContext, internalRow)(e, Operations.truncWeek)

      case TruncHourExpr(e) =>
        evaluateUnary(queryContext, internalRow)(e, Operations.truncHour)

      case TruncMinuteExpr(e) =>
        evaluateUnary(queryContext, internalRow)(e, Operations.truncMinute)

      case TruncSecondExpr(e) =>
        evaluateUnary(queryContext, internalRow)(e, Operations.truncSecond)

      case p @ PlusExpr(a, b) =>
        evaluateBinary(queryContext, internalRow)(a, b, p.numeric.plus)

      case TypeConvertExpr(tc, e) =>
        tc.convert(evaluateExpression(e, queryContext, internalRow))

      case AggregateExpr(_, e) =>
        evaluateExpression(e, queryContext, internalRow)

      case WindowFunctionExpr(_, e) =>
        evaluateExpression(e, queryContext, internalRow)

      case InExpr(e, vs) =>
        vs contains evaluateExpression(e, queryContext, internalRow)

      case NotInExpr(e, vs) =>
        val eValue = evaluateExpression(e, queryContext, internalRow)
        eValue != null && !vs.contains(eValue)

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
          val v = evaluateExpression(es(i), queryContext, internalRow)
          values(i) = v
          if (v == null) {
            success = false
          }
          i += 1
        }

        if (success) values else null

//      case x => throw new IllegalArgumentException(s"Unsupported expression $x")
    }

    // I cannot find a better solution to ensure compiler that concrete expr type Out is the same with expr.Out
    res.asInstanceOf[expr.Out]
  }

  private def evaluateUnary[A, O](qc: QueryContext, internalRow: InternalRow)(e: Expression.Aux[A], f: A => O): O = {
    val ev = evaluateExpression(e, qc, internalRow)
    if (ev != null) f(ev) else null.asInstanceOf[O]
  }

  private def evaluateBinary[A, B, O](
      qc: QueryContext,
      internalRow: InternalRow
  )(a: Expression.Aux[A], b: Expression.Aux[B], f: (A, B) => O): O = {
    val left = evaluateExpression(a, qc, internalRow)
    val right = evaluateExpression(b, qc, internalRow)
    if (left != null && right != null) {
      f(left, right)
    } else {
      null.asInstanceOf[O]
    }
  }
}
