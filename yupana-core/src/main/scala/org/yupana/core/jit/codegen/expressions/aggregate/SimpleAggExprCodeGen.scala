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

package org.yupana.core.jit.codegen.expressions.aggregate

import org.yupana.api.query.AggregateExpr
import org.yupana.core.jit.codegen.CommonGen
import org.yupana.core.jit.{CodeGenResult, Decl, ExpressionCodeGenFactory, State, ValueDeclaration}

import scala.reflect.runtime.universe._

trait SimpleAggExprCodeGen[T <: AggregateExpr[_, _, _]] extends AggregateExpressionCodeGen[T] {

  def zeroTree(innerExprValue: ValueDeclaration): Tree

  def foldTree(accValue: ValueDeclaration, innerExprValue: ValueDeclaration): Tree

  def combineTree(accValueA: ValueDeclaration, accValueB: ValueDeclaration): Tree

  def globalDeclarations(): Seq[Decl] = Seq.empty

  override def generateZeroCode(state: State, acc: TermName, row: TermName): CodeGenResult = {
    val dState = stateWithGlobalDeclarations(state)

    val r = ExpressionCodeGenFactory.codeGenerator(expression.expr).generateEvalCode(dState, row)
    val w = r.state.withWriteToRow(acc, expression)

    val valDecl = w.valueDeclaration
    val validityTree = q"val ${valDecl.validityFlagName} = true"
    val valueTree = if (expression.expr.isNullable) {
      q"val ${valDecl.valueName} = if (${r.valueDeclaration.validityFlagName}) ${zeroTree(r.valueDeclaration)} else ${CommonGen
          .initVal(expression)}"
    } else {
      q"val ${valDecl.valueName} = ${zeroTree(r.valueDeclaration)}"
    }
    CodeGenResult(r.trees ++ Seq(validityTree, valueTree), valDecl, w.state)
  }

  override def generateFoldCode(state: State, accRow: TermName, row: TermName): CodeGenResult = {
    val dState = stateWithGlobalDeclarations(state)

    val accRes = dState.withReadFromRow(accRow, expression)
    val rowRes = ExpressionCodeGenFactory.codeGenerator(expression.expr).generateEvalCode(accRes.state, row)

    val valDecl = ValueDeclaration(s"res_${accRes.valueDeclaration.valueName}")

    val funcTree = foldTree(accRes.valueDeclaration, rowRes.valueDeclaration)
    val validityTree =
      q"val ${valDecl.validityFlagName} = true"

    val valueTree = if (expression.expr.isNullable) {
      q""" val ${valDecl.valueName} =
                 if (${rowRes.valueDeclaration.validityFlagName}) {
                    $funcTree
                 } else {
                    ${accRes.valueDeclaration.valueName}
                 }
             """
    } else {
      q"val ${valDecl.valueName} = $funcTree"
    }

    val trees = accRes.trees ++ rowRes.trees ++ Seq(validityTree, valueTree)
    val s3 = rowRes.state.withWriteToRow(accRow, expression, valDecl)
    CodeGenResult(trees, valDecl, s3)
  }

  override def generateCombineCode(state: State, accRowA: TermName, accRowB: TermName): CodeGenResult = {
    val dState = stateWithGlobalDeclarations(state)

    val aRes = dState.withReadFromRow(accRowA, expression)
    val bRes = aRes.state.withReadFromRow(accRowB, expression)

    val rValue = ValueDeclaration(s"combine_${aRes.valueDeclaration.valueName}_${bRes.valueDeclaration.valueName}")

    val validityTree =
      q"val ${rValue.validityFlagName} = true"

    val valueTree =
      q"val ${rValue.valueName} = ${combineTree(aRes.valueDeclaration, bRes.valueDeclaration)}"

    val ns = bRes.state.withWriteToRow(accRowA, expression, rValue)
    val trees = aRes.trees ++ bRes.trees ++ Seq(validityTree, valueTree)
    CodeGenResult(trees, rValue, ns)
  }

  override def generatePostCombineCode(state: State, acc: TermName): CodeGenResult = {
    val r = state.withReadFromRow(acc, expression)
    val ns = r.state.withWriteToRow(acc, expression, r.valueDeclaration)
    r.copy(state = ns)
  }

  private def stateWithGlobalDeclarations(state: State) = {
    globalDeclarations().foldLeft(state) { (s, decl) =>
      s.withNamedGlobal(decl.name, decl.tpe, decl.value)
    }
  }
}
