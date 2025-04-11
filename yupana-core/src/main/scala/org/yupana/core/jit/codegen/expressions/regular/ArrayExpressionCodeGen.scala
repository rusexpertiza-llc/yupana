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

import org.yupana.api.query.ArrayExpr
import org.yupana.core.jit.codegen.expressions.ExpressionCodeGen
import org.yupana.core.jit.{ ExpressionCodeGenFactory, CodeGenResult, State }

import scala.reflect.runtime.universe._

trait ArrayExpressionCodeGen extends ExpressionCodeGen[ArrayExpr[_]] {

  override def generateEvalCode(state: State, row: TermName): CodeGenResult = {
    val (valDecl, exprState) = state.withLocalValueDeclaration(expression)

    val (rs, newState) = expression.exprs.zipWithIndex.foldLeft((Seq.empty[CodeGenResult], exprState)) {
      case ((acc, state), (e, i)) =>
        val r = ExpressionCodeGenFactory.codeGenerator(e).generateEvalCode(state, row)
        (acc :+ r, r.state)
    }

    val vs = rs.map { r =>
      q"${r.valueDeclaration.valueName}"
    }

    val validityTree = q"val ${valDecl.validityFlagName} = true"
    val valTree = q"val ${valDecl.valueName} = Seq(..$vs)"
    val trees = rs.flatMap(_.trees) ++ Seq(validityTree, valTree)
    CodeGenResult(trees, valDecl, newState)
  }
}

object ArrayExpressionCodeGen {
  def apply(expr: ArrayExpr[_]): ArrayExpressionCodeGen = {
    new ArrayExpressionCodeGen() {
      override def expression: ArrayExpr[_] = expr
    }
  }
}
