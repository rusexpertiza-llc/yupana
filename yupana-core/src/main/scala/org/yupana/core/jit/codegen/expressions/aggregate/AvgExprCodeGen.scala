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

import org.yupana.api.Currency
import org.yupana.api.query._
import org.yupana.core.jit.codegen.CommonGen
import org.yupana.core.jit.codegen.expressions.regular.CodegenUtils._
import org.yupana.core.jit.{ CodeGenResult, State }

import scala.reflect.runtime.universe._

class AvgExprCodeGen(override val expression: AvgExpr[_]) extends AggregateExpressionCodeGen[AvgExpr[_]] {
  private val sumExpr = avgSumAccumulatorExpr(expression.expr)
  private val countExpr = CountExpr(expression.expr)

  private val sumCodeGen = new SumExprCodeGen(sumExpr)
  private val countCodeGen = new CountExprCodeGen(countExpr)
  override def generateZeroCode(state: State, acc: TermName, row: TermName): CodeGenResult = {
    val sum = sumCodeGen.generateZeroCode(state, acc, row)
    val count = countCodeGen.generateZeroCode(sum.state, acc, row)
    CodeGenResult(sum.trees ++ count.trees, count.valueDeclaration, count.state)
  }

  override def generateFoldCode(state: State, accRow: TermName, row: TermName): CodeGenResult = {
    val sumRes = sumCodeGen.generateFoldCode(state, accRow, row)
    val countRes = countCodeGen.generateFoldCode(sumRes.state, accRow, row)
    CodeGenResult(sumRes.trees ++ countRes.trees, countRes.valueDeclaration, countRes.state)
  }

  override def generateCombineCode(state: State, accRowA: TermName, accRowB: TermName): CodeGenResult = {
    val sumRes = sumCodeGen.generateCombineCode(state, accRowA, accRowB)
    val countRes = countCodeGen.generateCombineCode(sumRes.state, accRowA, accRowB)
    CodeGenResult(sumRes.trees ++ countRes.trees, countRes.valueDeclaration, countRes.state)
  }

  override def generatePostCombineCode(state: State, acc: TermName): CodeGenResult = {
    val w = state.withWriteToRow(acc, expression)

    val avgVal = w.valueDeclaration
    val sum = w.state.withReadFromRow(acc, sumExpr)
    val count = sum.state.withReadFromRow(acc, countExpr)

    val validityTree = q"val ${avgVal.validityFlagName} = ${count.valueDeclaration.valueName} > 0"

    val evaluateValue = if (isDecimal(expression.expr)) {
      q"${sum.valueDeclaration.valueName} / BigDecimal(${count.valueDeclaration.valueName})"
    } else if (isCurrency(expression.expr)) {
      q"${sum.valueDeclaration.valueName}.toBigDecimal / BigDecimal(${count.valueDeclaration.valueName})"
    } else {
      q"BigDecimal(${sum.valueDeclaration.valueName}.toDouble / ${count.valueDeclaration.valueName})"
    }

    val valueTree =
      q"""val ${avgVal.valueName} =
              if (${sum.valueDeclaration.validityFlagName} && ${count.valueDeclaration.valueName} > 0)
              $evaluateValue
              else null
          """

    val trees = sum.trees ++ count.trees ++ Seq(validityTree, valueTree)
    CodeGenResult(trees, avgVal, count.state)
  }

  private def avgSumAccumulatorExpr(expr: Expression[_]): SumExpr[_, _] = {
    CommonGen.className(expr.dataType) match {
      case "Byte"       => SumExpr[Byte, Int](expr.asInstanceOf[Expression[Byte]])
      case "Short"      => SumExpr[Short, Int](expr.asInstanceOf[Expression[Short]])
      case "Int"        => SumExpr[Int, Int](expr.asInstanceOf[Expression[Int]])
      case "Long"       => SumExpr[Long, Long](expr.asInstanceOf[Expression[Long]])
      case "Double"     => SumExpr[Double, Double](expr.asInstanceOf[Expression[Double]])
      case "BigDecimal" => SumExpr[BigDecimal, BigDecimal](expr.asInstanceOf[Expression[BigDecimal]])
      case "Currency"   => SumExpr[Currency, Currency](expr.asInstanceOf[Expression[Currency]])
      case x            => throw new IllegalStateException(s"$x type is not available for Avg expression")
    }
  }
}
