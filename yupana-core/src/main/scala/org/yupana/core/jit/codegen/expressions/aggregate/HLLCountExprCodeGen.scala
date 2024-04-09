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

import org.yupana.api.query.HLLCountExpr
import org.yupana.core.jit.codegen.CommonGen
import org.yupana.core.jit.{ CodeGenResult, ExpressionCodeGenFactory, State, ValueDeclaration }

import scala.reflect.runtime.universe._

class HLLCountExprCodeGen(override val expression: HLLCountExpr[_])
    extends AggregateExpressionCodeGen[HLLCountExpr[_]] {

  val hllType: Tree = tq"_root_.com.twitter.algebird.HLL"

  override def generateZeroCode(state: State, acc: TermName, row: TermName): CodeGenResult = {
    val r = ExpressionCodeGenFactory.codeGenerator(expression.expr).generateEvalCode(state, row)
    val w = r.state.withWriteRefToRow(acc, expression)
    val valDecl = w.valueDeclaration

    val tpe = CommonGen.mkType(expression.expr)
    val validityTree = q"val ${valDecl.validityFlagName} = true"
    val instance = q"_root_.com.twitter.algebird.HyperLogLogAggregator.withErrorGeneric[$tpe](${expression.accuracy})"
    val withValue =
      q"""{
             val agg = $instance
             agg.prepare(${r.valueDeclaration.valueName})
           }"""

    val valueTree = if (expression.expr.isNullable) {
      q"""val ${valDecl.valueName} = if (${r.valueDeclaration.validityFlagName}) {
                 $withValue
               } else {
                  val agg = $instance
                  agg.monoid.empty
               }
          """
    } else {
      q"val ${valDecl.valueName} = $withValue"
    }

    CodeGenResult(r.trees ++ Seq(validityTree, valueTree), valDecl, w.state)
  }

  override def generateFoldCode(state: State, accRow: TermName, row: TermName): CodeGenResult = {
    val accRes = state.withReadRefFromRow(accRow, expression, hllType)
    val rowRes = ExpressionCodeGenFactory.codeGenerator(expression.expr).generateEvalCode(accRes.state, row)

    val valDecl = ValueDeclaration(s"res_${accRes.valueDeclaration.valueName}")
    val tpe = CommonGen.mkType(expression.expr.dataType)
    val validityTree = q"val ${valDecl.validityFlagName} = true"
    val valueTree =
      q"""
         val ${valDecl.valueName} = if (${rowRes.valueDeclaration.validityFlagName}) {
            val agg = _root_.com.twitter.algebird.HyperLogLogAggregator.withErrorGeneric[$tpe](${expression.accuracy})
            val a = ${accRes.valueDeclaration.valueName}
            agg.append(a, ${rowRes.valueDeclaration.valueName})
         } else {
            ${accRes.valueDeclaration.valueName}
         }
       """

    val s3 = rowRes.state.withWriteRefToRow(accRow, expression, valDecl)
    val trees = accRes.trees ++ rowRes.trees ++ Seq(validityTree, valueTree)
    CodeGenResult(trees, valDecl, s3)
  }

  override def generateCombineCode(state: State, accRowA: TermName, accRowB: TermName): CodeGenResult = {

    val aRes = state.withReadRefFromRow(accRowA, expression, hllType)
    val bRes = aRes.state.withReadRefFromRow(accRowB, expression, hllType)

    val valDecl = ValueDeclaration(s"combine_${aRes.valueDeclaration.valueName}_${bRes.valueDeclaration.valueName}")
    val validityTree = q"val ${valDecl.validityFlagName} = true"
    val valueTpe = CommonGen.mkType(expression.expr)
    val valueTree =
      q"""val ${valDecl.valueName} = {
                val agg = _root_.com.twitter.algebird.HyperLogLogAggregator.withErrorGeneric[$valueTpe](${expression.accuracy})
                val hll = agg.monoid
                val a = ${aRes.valueDeclaration.valueName}
                val b = ${bRes.valueDeclaration.valueName}
                hll.combine(a, b)
           }"""

    val ns = bRes.state.withWriteRefToRow(accRowA, expression, valDecl)
    val trees = aRes.trees ++ bRes.trees ++ Seq(validityTree, valueTree)
    CodeGenResult(trees, valDecl, ns)
  }

  override def generatePostCombineCode(state: State, acc: TermName): CodeGenResult = {

    val r = state.withReadRefFromRow(acc, expression, hllType)

    val valDecl = ValueDeclaration(s"post_comb_${r.valueDeclaration.valueName}")

    val validityTree = q"val ${valDecl.validityFlagName} = true"
    val valueTree =
      q"val ${valDecl.valueName} = ${r.valueDeclaration.valueName}.approximateSize.estimate"

    val trees = r.trees ++ Seq(validityTree, valueTree)
    val newState = r.state.withWriteToRow(acc, expression, valDecl)
    CodeGenResult(trees, valDecl, newState)
  }
}
