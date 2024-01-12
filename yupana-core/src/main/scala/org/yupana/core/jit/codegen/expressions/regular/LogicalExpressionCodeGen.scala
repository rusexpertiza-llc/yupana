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

import org.yupana.api.query.Expression.Condition
import org.yupana.api.query.{ AndExpr, Expression, OrExpr }
import org.yupana.core.jit.codegen.expressions.ExpressionCodeGen
import org.yupana.core.jit.{ ExpressionCodeGenFactory, CodeGenResult, State, ValueDeclaration }

import scala.reflect.runtime.universe._

trait LogicalExpressionCodeGen extends ExpressionCodeGen[Expression[Boolean]] {

  def conditions: Seq[Condition]

  def reducer(valueA: Tree, valueB: Tree): Tree

  override def generateEvalCode(state: State, row: TermName): CodeGenResult = {
    val exprState = state.withExpression(expression)
    val index = exprState.index(expression)

    val head = ExpressionCodeGenFactory.codeGenerator(conditions.head).generateEvalCode(exprState, row)

    conditions.tail.zipWithIndex.foldLeft(head) {
      case (acc, (cond, i)) =>
        val valDecl = ValueDeclaration(s"cond_${index}_$i")

        val r = ExpressionCodeGenFactory.codeGenerator(cond).generateEvalCode(acc.state, row)

        val validityTree =
          q"val ${valDecl.validityFlagName} = ${acc.valueDeclaration.validityFlagName} && ${r.valueDeclaration.validityFlagName}"

        val reduceTree = reducer(q"${acc.valueDeclaration.valueName}", q"${r.valueDeclaration.valueName}")

        val valueTree = if (expression.isNullable) {
          q"""
             val ${valDecl.valueName} = if (${valDecl.validityFlagName}) {
                $reduceTree
             } else {
                false
             }
             """
        } else {
          q"val ${valDecl.valueName} = $reduceTree"
        }

        val trees = acc.trees ++ r.trees ++ Seq(validityTree, valueTree)
        CodeGenResult(trees, valDecl, r.state)
    }
  }
}

object LogicalExpressionCodeGen {

  def and(expr: AndExpr): LogicalExpressionCodeGen = {
    apply(expr, expr.conditions, (a, b) => q"$a && $b")
  }

  def or(expr: OrExpr): LogicalExpressionCodeGen = {
    apply(expr, expr.conditions, (a, b) => q"$a || $b")
  }

  def apply(expr: Expression[Boolean], conds: Seq[Condition], code: (Tree, Tree) => Tree): LogicalExpressionCodeGen = {
    new LogicalExpressionCodeGen() {
      override def conditions: Seq[Condition] = conds
      override def reducer(valueA: Tree, valueB: Tree): Tree = code(valueA, valueB)
      override def expression: Expression[Boolean] = expr
    }
  }
}
