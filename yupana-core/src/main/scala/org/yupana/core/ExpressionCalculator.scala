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

import org.yupana.api.query._
import org.yupana.core.model.InternalRow

trait ConstantCalculator {
  def evaluateConstant[T](expr: Expression[T]): T
}

trait ExpressionCalculator {
  def preEvaluated[T](expr: Expression[T], queryContext: QueryContext, internalRow: InternalRow): T = {
    expr match {
      case ConstantExpr(v) => v
      case _               => internalRow.get[T](queryContext, expr)
    }
  }

  def evaluateExpression[T](expr: Expression[T], queryContext: QueryContext, internalRow: InternalRow): T

  def evaluateMap[I, M](expr: AggregateExpr[I, M, _], queryContext: QueryContext, row: InternalRow): M
  def evaluateReduce[M](expr: AggregateExpr[_, M, _], queryContext: QueryContext, a: InternalRow, b: InternalRow): M
  def evaluatePostMap[M, O](expr: AggregateExpr[_, M, O], queryContext: QueryContext, row: InternalRow): O

  def evaluateWindow[I, O](winFuncExpr: WindowFunctionExpr[I, O], values: Array[I], index: Int): O
}

object ExpressionCalculator {
  import scala.reflect.runtime.universe._
  import scala.reflect.runtime.currentMirror
  import scala.tools.reflect.ToolBox

  def generateCalculator(queryContext: QueryContext): Tree = {
    q"""new _root_.org.yupana.core.ExpressionCalculator {
          def evaluateConstant[T](expr: _root_.org.yupana.api.query.Expression[T]): T = ???

          def evaluateExpression[T](
            expr: _root_.org.yupana.api.query.Expression[T], 
            queryContext: _root_.org.yupana.core.QueryContext, 
            internalRow: _root_.org.yupana.core.model.InternalRow
          ): T = ???

          def evaluateMap[I, M](
            expr: _root_.org.yupana.api.query.AggregateExpr[I, M, _],
            queryContext: _root_.org.yupana.core.QueryContext, 
            row: _root_.org.yupana.core.model.InternalRow
          ): M = ???

          def evaluateReduce[M](
            expr: _root_.org.yupana.api.query.AggregateExpr[_, M, _],
            queryContext: _root_.org.yupana.core.QueryContext,
            a: _root_.org.yupana.core.model.InternalRow,
            b: _root_.org.yupana.core.model.InternalRow
          ): M = ???

          def evaluatePostMap[M, O](
            expr: _root_.org.yupana.api.query.AggregateExpr[_, M, O], 
            queryContext: _root_.org.yupana.core.QueryContext, 
            row: _root_.org.yupana.core.model.InternalRow
          ): O = ???

          def evaluateWindow[I, O](winFuncExpr: _root_.org.yupana.api.query.WindowFunctionExpr[I, O], values: Array[I], index: Int): O = ???
        }
    """
  }

  def makeCalculator(queryContext: QueryContext): ExpressionCalculator = {
    val tb = currentMirror.mkToolBox()

    val tree = generateCalculator(queryContext)

    tb.compile(tree)().asInstanceOf[ExpressionCalculator]
  }

}
