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

package org.yupana.core.jit.codegen.expressions.regular

import org.yupana.api.query.BinaryOperationExpr
import org.yupana.core.jit.codegen.CommonGen
import org.yupana.core.jit.codegen.expressions.ExpressionCodeGen
import org.yupana.core.jit.{ CodeGenResult, State }

import scala.reflect.runtime.universe._

trait OrdExpressionCodeGen[E <: BinaryOperationExpr[_, _, Boolean]] extends ExpressionCodeGen[E] {

  def tree(operandA: Tree, operandB: Tree): Tree

  def ordFunctionName: String

  override def generateEvalCode(state: State, row: TermName): CodeGenResult = {
    if (expression.operandA.dataType.num.nonEmpty) {
      BinaryExpressionCodeGen(expression, tree).generateEvalCode(state, row)
    } else {
      val aType = CommonGen.mkType(expression.operandA)
      val ord = CommonGen.ordValName(expression.operandA.dataType)
      val newState = state.withNamedGlobal(
        ord,
        tq"Ordering[$aType]",
        q"DataType.bySqlName(${expression.operandA.dataType.meta.sqlTypeName}).get.asInstanceOf[DataType.Aux[$aType]].ordering.get"
      )
      BinaryExpressionCodeGen(expression, (x, y) => q"$ord.${TermName(ordFunctionName)}($x, $y)")
        .generateEvalCode(newState, row)
    }
  }
}

object OrdExpressionCodeGen {
  def apply[E <: BinaryOperationExpr[_, _, Boolean]](
      expr: E,
      code: (Tree, Tree) => Tree,
      ordFunName: String
  ): OrdExpressionCodeGen[E] = {
    new OrdExpressionCodeGen[E] {
      override def expression: E = expr
      override def tree(operandA: Tree, operandB: Tree): Tree = code(operandA, operandB)
      override def ordFunctionName: String = ordFunName
    }
  }
}
