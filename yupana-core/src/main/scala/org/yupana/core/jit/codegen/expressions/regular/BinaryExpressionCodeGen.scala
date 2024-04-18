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
import org.yupana.core.jit.{ ExpressionCodeGenFactory, CodeGenResult, State }

import scala.reflect.runtime.universe._

trait BinaryExpressionCodeGen[E <: BinaryOperationExpr[_, _, _]] extends ExpressionCodeGen[E] {

  def tree(operandA: Tree, operandB: Tree): Tree

  override def generateEvalCode(state: State, row: TermName): CodeGenResult = {
    val (valDecl, exprState) = state.withLocalValueDeclaration(expression)

    val ra = ExpressionCodeGenFactory.codeGenerator(expression.operandA).generateEvalCode(exprState, row)
    val rb = ExpressionCodeGenFactory.codeGenerator(expression.operandB).generateEvalCode(ra.state, row)

    val validityTree =
      q"val ${valDecl.validityFlagName} = ${ra.valueDeclaration.validityFlagName} && ${rb.valueDeclaration.validityFlagName}"

    val tpe = CommonGen.mkType(expression)
    val exprTree = tree(q"${ra.valueDeclaration.valueName}", q"${rb.valueDeclaration.valueName}")
    val typedExprTree = q"($exprTree).asInstanceOf[$tpe]"
    val valueTree = if (expression.isNullable) {
      q"""
           val ${valDecl.valueName} = if (${valDecl.validityFlagName}) {
              $typedExprTree
           } else {
              ${CommonGen.initVal(expression)}
           }
         """
    } else {
      q"val ${valDecl.valueName} = $typedExprTree"
    }
    val trees = ra.trees ++ rb.trees ++ Seq(validityTree, valueTree)
    CodeGenResult(trees, valDecl, rb.state)
  }
}

object BinaryExpressionCodeGen {

  def apply[E <: BinaryOperationExpr[_, _, _]](expr: E, code: (Tree, Tree) => Tree): BinaryExpressionCodeGen[E] = {
    new BinaryExpressionCodeGen[E] {

      override def expression: E = expr

      override def tree(operandA: Tree, operandB: Tree): Tree = code(operandA, operandB)
    }
  }
}
