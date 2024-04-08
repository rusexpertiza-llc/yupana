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

import org.yupana.api.query.{AggregateExpr, DistinctCountExpr, DistinctRandomExpr}
import org.yupana.core.jit.codegen.CommonGen
import org.yupana.core.jit.codegen.CommonGen.className
import org.yupana.core.jit.{CodeGenResult, ExpressionCodeGenFactory, State, ValueDeclaration}

import scala.reflect.runtime.universe._

trait DistinctExprsCodeGen extends AggregateExpressionCodeGen[AggregateExpr[_, _, _]] {

  val setType = {
    AppliedTypeTree(
      Ident(TypeName("Set")),
      List(Ident(TypeName(className(expression.expr.dataType))))
    )
  }

  override def generateZeroCode(state: State, acc: TermName, row: TermName): CodeGenResult = {
    val r = ExpressionCodeGenFactory.codeGenerator(expression.expr).generateEvalCode(state, row)
    val w = r.state.withWriteRefToRow(acc, expression)

    val valDecl = w.valueDeclaration
    val tpe = CommonGen.mkType(expression.expr)
    val validityTree = q"val ${valDecl.validityFlagName} = true"
    val valueTree = if (expression.expr.isNullable) {
      q"""val ${valDecl.valueName}: $setType = if (${r.valueDeclaration.validityFlagName}) {
                  Set(${r.valueDeclaration.valueName})
               } else {
                  Set.empty[$tpe]
               }
          """
    } else {
      q"val ${valDecl.valueName}: $setType = Set(${r.valueDeclaration.valueName})"
    }

    CodeGenResult(r.trees ++ Seq(validityTree, valueTree), valDecl, w.state)
  }

  override def generateFoldCode(state: State, accRow: TermName, row: TermName): CodeGenResult = {

    val accRes = state.withReadRefFromRow(accRow, expression, q"$setType")
    val rowRes = ExpressionCodeGenFactory.codeGenerator(expression.expr).generateEvalCode(accRes.state, row)
    val writeRes = rowRes.state.withWriteRefToRow(accRow, expression)

    val valDecl = writeRes.valueDeclaration
    val validityTree = q"val ${valDecl.validityFlagName} = true"
    val valueTree =
      q"""
               val ${valDecl.valueName} = if (${rowRes.valueDeclaration.validityFlagName}) {
                  ${accRes.valueDeclaration.valueName} + ${rowRes.valueDeclaration.valueName}
               } else {
                  ${accRes.valueDeclaration.valueName}
               }
             """
    val trees = accRes.trees ++ rowRes.trees ++ Seq(validityTree, valueTree)
    CodeGenResult(trees, valDecl, writeRes.state)
  }

  override def generateCombineCode(
      state: State,
      accRowA: TermName,
      accRowB: TermName
  ): CodeGenResult = {
    val aRes = state.withReadRefFromRow(accRowA, expression, q"$setType")
    val bRes = aRes.state.withReadRefFromRow(accRowB, expression, q"$setType")
    val wRes = bRes.state.withWriteRefToRow(accRowA, expression)
    val valDecl = wRes.valueDeclaration

    val validityTree = q"val ${valDecl.validityFlagName} = true"

    val valueTree =
      q"val ${valDecl.valueName} = ${aRes.valueDeclaration.valueName} ++ ${bRes.valueDeclaration.valueName}"

    val trees = aRes.trees ++ bRes.trees ++ Seq(validityTree, valueTree)
    CodeGenResult(trees, valDecl, wRes.state)
  }

}

object DistinctExprsCodeGen {

  def distinctCount(expr: DistinctCountExpr[_]): DistinctExprsCodeGen = {
    new DistinctExprsCodeGen {

      override def expression: AggregateExpr[_, _, _] = expr

      override def generatePostCombineCode(state: State, acc: TermName): CodeGenResult = {
        val r = state.withReadRefFromRow(acc, expression, q"$setType")

        val valDecl = ValueDeclaration(s"post_comb_${r.valueDeclaration.valueName}")


        val validityTree = q"val ${valDecl.validityFlagName} = true"
        val valueTree = q"val ${valDecl.valueName} = ${r.valueDeclaration.valueName}.size"

        val trees = r.trees ++ Seq(validityTree, valueTree)
        val newState = r.state.withWriteToRow(acc, expression, valDecl)
        CodeGenResult(trees, valDecl, newState)
      }
    }
  }

  def distinctRandom(expr: DistinctRandomExpr[_]): DistinctExprsCodeGen = {
    new DistinctExprsCodeGen {

      override def expression: AggregateExpr[_, _, _] = expr

      override def generatePostCombineCode(state: State, acc: TermName): CodeGenResult = {
        val r = state.withReadRefFromRow(acc, expression, q"$setType")

        val valDecl = ValueDeclaration(s"post_comb_${r.valueDeclaration.valueName}")

        val validityTree = q"val ${valDecl.validityFlagName} = true"
        val valueTree =
          q"""
             val ${valDecl.valueName} = {
               val s = ${r.valueDeclaration.valueName}
               val n = _root_.scala.util.Random.nextInt(s.size)
               s.iterator.drop(n).next
             }
             """
        val trees = r.trees ++ Seq(validityTree, valueTree)
        val newState = r.state.withWriteToRow(acc, expression, valDecl)
        CodeGenResult(trees, valDecl, newState)
      }
    }
  }
}
