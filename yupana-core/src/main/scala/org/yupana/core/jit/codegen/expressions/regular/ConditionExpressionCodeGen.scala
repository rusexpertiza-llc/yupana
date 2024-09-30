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

import org.yupana.api.query.ConditionExpr
import org.yupana.core.jit.codegen.expressions.ExpressionCodeGen
import org.yupana.core.jit.{ ExpressionCodeGenFactory, CodeGenResult, State }

import scala.reflect.runtime.universe._

trait ConditionExpressionCodeGen extends ExpressionCodeGen[ConditionExpr[_]] {

  override def generateEvalCode(state: State, row: TermName): CodeGenResult = {
    val (valDecl, exprState) = state.withLocalValueDeclaration(expression)

    val ifRes = ExpressionCodeGenFactory
      .codeGenerator(expression.condition)
      .generateEvalCode(exprState, row)

    val thenRes = ExpressionCodeGenFactory
      .codeGenerator(expression.positive)
      .generateEvalCode(ifRes.state, row)

    val elseRes = ExpressionCodeGenFactory
      .codeGenerator(expression.negative)
      .generateEvalCode(thenRes.state.withLocalValues(ifRes.state), row)

    val ifCondTree = if (expression.condition.isNullable) {
      q"${ifRes.valueDeclaration.validityFlagName} && ${ifRes.valueDeclaration.valueName}"
    } else {
      q"${ifRes.valueDeclaration.valueName}"
    }

    val valueTree =
      q"""
        val ${valDecl.valueName} = if ($ifCondTree) {
           ..${thenRes.trees}
           ${thenRes.valueDeclaration.valueName}
        } else {
           ..${elseRes.trees}
           ${elseRes.valueDeclaration.valueName}
        }
    """

    val validityTree =
      q"""
        val ${valDecl.validityFlagName} = if ($ifCondTree) {
           ..${thenRes.trees}
           ${thenRes.valueDeclaration.validityFlagName}
        } else {
           ..${elseRes.trees}
           ${elseRes.valueDeclaration.validityFlagName}
        }
    """

    CodeGenResult(ifRes.trees ++ Seq(validityTree, valueTree), valDecl, elseRes.state.withLocalValues(ifRes.state))
  }
}

object ConditionExpressionCodeGen {
  def apply(expr: ConditionExpr[_]): ConditionExpressionCodeGen = {
    new ConditionExpressionCodeGen() {
      override def expression: ConditionExpr[_] = expr
    }
  }
}