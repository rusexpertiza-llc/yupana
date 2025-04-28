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
import org.yupana.api.types.DataType
import org.yupana.core.jit.codegen.CommonGen.mkType
import org.yupana.core.jit.codegen.expressions.ExpressionCodeGen
import org.yupana.core.jit.{ CodeGenResult, State }

import scala.reflect.runtime.universe._

trait MathUnaryExpressionCodeGen extends ExpressionCodeGen[UnaryOperationExpr[_, _]] {

  def mathFunction: TermName

  override def generateEvalCode(state: State, row: TermName): CodeGenResult = {
    val aType = mkType(expression)
    if (expression.dataType.num.nonEmpty) {

      val r = UnaryExpressionCodeGen(expression, x => q"${numValName(expression.dataType)}.$mathFunction($x)")
        .generateEvalCode(state, row)

      val newState = r.state.withNamedGlobal(
        numValName(expression.dataType),
        tq"Num[$aType]",
        q"DataType.bySqlName(${expression.dataType.meta.sqlTypeName}).get.asInstanceOf[DataType.Aux[$aType]].num.get"
      )
      r.copy(state = newState)
    } else {
      throw new IllegalArgumentException(s"Cannot use math operations because $expression is not numeric")
    }
  }

  private def numValName(dt: DataType): TermName = {
    TermName(s"int_${dt.toString}")
  }
}

object MathUnaryExpressionCodeGen {
  def apply(expr: UnaryOperationExpr[_, _], fun: TermName): MathUnaryExpressionCodeGen = {
    new MathUnaryExpressionCodeGen() {
      override def mathFunction: TermName = fun
      override def expression: UnaryOperationExpr[_, _] = expr
    }
  }
}
