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

import org.yupana.api.query.{ UnaryOperationExpr, ValueExpr }
import org.yupana.core.jit.codegen.CommonGen
import org.yupana.core.jit.codegen.expressions.ExpressionCodeGen
import org.yupana.core.jit.{ CodeGenResult, State }
import org.yupana.core.utils.ConditionUtils

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

trait ConditionWithRefCodeGen extends ExpressionCodeGen[UnaryOperationExpr[_, Boolean]] {

  def tree(value: Tree, ref: Tree): Tree

  def refValue: Set[Any]

  override def generateEvalCode(state: State, row: TermName): CodeGenResult = {
    val (n, ns) = state.withRef(refValue, tq"Set[${CommonGen.mkType(expression.operand)}]")
    UnaryExpressionCodeGen(expression, x => tree(x, q"$n")).generateEvalCode(ns, row)
  }
}

object ConditionWithRefCodeGen {
  def apply(
      expr: UnaryOperationExpr[_, Boolean],
      ref: Set[ValueExpr[Any]],
      code: (Tree, Tree) => Tree
  ): ConditionWithRefCodeGen = {
    new ConditionWithRefCodeGen() {
      override def expression: UnaryOperationExpr[_, Boolean] = expr
      override def refValue: Set[Any] = ref.map(ConditionUtils.value)
      override def tree(value: Tree, ref: Tree): universe.Tree = code(value, ref)
    }
  }
}
