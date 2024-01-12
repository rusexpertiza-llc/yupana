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

import org.yupana.api.query.{ AggregateExpr, DistinctCountExpr, DistinctRandomExpr }
import org.yupana.core.jit.codegen.CommonGen
import org.yupana.core.jit.{ CodeGenResult, State, ValueDeclaration }

import scala.reflect.runtime.universe._

trait DistinctExprsCodeGen extends AggregateExpressionCodeGen[AggregateExpr[_, _, _]] {
  override def generateZeroCode(state: State, row: TermName): CodeGenResult = {
    val r = state.withReadFromRow(row, expression.expr)
    val w = r.state.withWriteRefToRow(row, expression)

    val valDecl = w.valueDeclaration
    val tpe = CommonGen.mkType(expression.expr)
    val validityTree = q"val ${valDecl.validityFlagName} = true"
    val valueTree = if (expression.expr.isNullable) {
      q"""val ${valDecl.valueName} = if (${r.valueDeclaration.validityFlagName}) {
                 Set(${r.valueDeclaration.valueName})
               } else {
                  Set.empty[$tpe]
               }
          """
    } else {
      q"val ${valDecl.valueName} = Set(${r.valueDeclaration.valueName})"
    }

    CodeGenResult(r.trees ++ Seq(validityTree, valueTree), valDecl, w.state)
  }

  override def generateFoldCode(state: State, accRow: TermName, row: TermName): CodeGenResult = {
    val accRes = state.withReadRefFromRow(accRow, expression)
    val rowRes = accRes.state.withReadFromRow(row, expression.expr)
    val writeRes = rowRes.state.withWriteRefToRow(accRow, expression)

    val valDecl = writeRes.valueDeclaration
    val tpe = CommonGen.mkType(expression.expr.dataType)
    val validityTree = q"val ${valDecl.validityFlagName} = true"
    val valueTree =
      q"""
               val ${valDecl.valueName} = if (${rowRes.valueDeclaration.validityFlagName}) {
                  ${accRes.valueDeclaration.valueName}.asInstanceOf[Set[$tpe]] + ${rowRes.valueDeclaration.valueName}
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
    val aRes = state.withReadRefFromRow(accRowA, expression)
    val bRes = aRes.state.withReadRefFromRow(accRowB, expression)
    val wRes = bRes.state.withWriteRefToRow(accRowA, expression)

    val valDecl = wRes.valueDeclaration

    val validityTree = q"val ${valDecl.validityFlagName} = true"

    val valueTpe = CommonGen.mkType(expression.expr)
    val valueTree =
      q"val ${valDecl.valueName} = ${aRes.valueDeclaration.valueName}.asInstanceOf[Set[$valueTpe]] ++ ${bRes.valueDeclaration.valueName}.asInstanceOf[Set[$valueTpe]]"

    val trees = aRes.trees ++ bRes.trees ++ Seq(validityTree, valueTree)
    CodeGenResult(trees, valDecl, wRes.state)
  }

}

object DistinctExprsCodeGen {

  def distinctCount(expr: DistinctCountExpr[_]): DistinctExprsCodeGen = {
    new DistinctExprsCodeGen {

      override def expression: AggregateExpr[_, _, _] = expr

      override def generatePostCombineCode(state: State, acc: TermName): CodeGenResult = {
        val r = state.withReadRefFromRow(acc, expression)

        val valDecl = ValueDeclaration(s"post_comb_${r.valueDeclaration.valueName}")
        val tpe = CommonGen.mkType(expression.expr.dataType)

        val validityTree = q"val ${valDecl.validityFlagName} = true"
        val valueTree = q"val ${valDecl.valueName} = ${r.valueDeclaration.valueName}.asInstanceOf[Set[$tpe]].size"

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
        val r = state.withReadRefFromRow(acc, expression)

        val valDecl = ValueDeclaration(s"post_comb_${r.valueDeclaration.valueName}")
        val tpe = CommonGen.mkType(expression.expr.dataType)

        val validityTree = q"val ${valDecl.validityFlagName} = true"
        val valueTree =
          q"""
             val ${valDecl.valueName} = {
               val s = ${r.valueDeclaration.valueName}.asInstanceOf[Set[$tpe]]
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
