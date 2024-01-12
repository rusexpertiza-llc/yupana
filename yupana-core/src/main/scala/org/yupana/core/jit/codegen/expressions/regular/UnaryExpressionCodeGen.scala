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

import org.yupana.api.query.UnaryOperationExpr
import org.yupana.core.jit.codegen.CommonGen
import org.yupana.core.jit.codegen.CommonGen.mkType
import org.yupana.core.jit.codegen.expressions.ExpressionCodeGen
import org.yupana.core.jit.{ ExpressionCodeGenFactory, CodeGenResult, State }

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

trait UnaryExpressionCodeGen[E <: UnaryOperationExpr[_, _]] extends ExpressionCodeGen[E] {

  def tree(operand: Tree): Tree

  def elseTree: Option[Tree]

  override def generateEvalCode(state: State, row: TermName): CodeGenResult = {
    val (valDecl, exprState) = state.withLocalValueDeclaration(expression)

    val ra = ExpressionCodeGenFactory.codeGenerator(expression.operand).generateEvalCode(exprState, row)

    val validityTree = if (!expression.isNullable || elseTree.isDefined) {
      q"val ${valDecl.validityFlagName} = true"
    } else {
      q"val ${valDecl.validityFlagName} = ${ra.valueDeclaration.validityFlagName}"
    }

    val tpe = mkType(expression)
    val exprTree = q"${tree(q"${ra.valueDeclaration.valueName}")}.asInstanceOf[$tpe]"

    val notValidTree = elseTree.getOrElse(CommonGen.initVal(expression))

    val valueTree = if (expression.isNullable) {
      q"""
          val ${valDecl.valueName} = if (${ra.valueDeclaration.validityFlagName}) {
             $exprTree
          } else {
             $notValidTree
          }
         """
    } else {
      q"val ${valDecl.valueName} = $exprTree"
    }

    val trees = ra.trees ++ Seq(validityTree, valueTree)
    CodeGenResult(trees, valDecl, ra.state)
  }
}

object UnaryExpressionCodeGen {
  def apply[E <: UnaryOperationExpr[_, _]](
      expr: E,
      code: Tree => Tree,
      elseCode: Option[Tree] = None
  ): UnaryExpressionCodeGen[E] = {
    new UnaryExpressionCodeGen[E] {

      override def expression: E = expr

      override def tree(operand: universe.Tree): universe.Tree = code(operand)

      override def elseTree: Option[universe.Tree] = elseCode
    }
  }
}
