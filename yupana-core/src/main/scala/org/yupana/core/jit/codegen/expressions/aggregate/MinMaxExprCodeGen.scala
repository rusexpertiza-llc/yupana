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

import org.yupana.api.query.{ AggregateExpr, MaxExpr, MinExpr }
import org.yupana.core.jit.codegen.CommonGen
import org.yupana.core.jit.{ Decl, ValueDeclaration }

import scala.reflect.runtime.universe._

trait MinMaxExprCodeGen extends SimpleAggExprCodeGen[AggregateExpr[_, _, _]] {

  override def globalDeclarations(): Seq[Decl] = {
    val aType = CommonGen.mkType(expression.expr)

    Seq(
      Decl(
        CommonGen.ordValName(expression.expr.dataType),
        tq"Ordering[$aType]",
        q"DataType.bySqlName(${expression.expr.dataType.meta.sqlTypeName}).get.asInstanceOf[DataType.Aux[$aType]].ordering.get"
      )
    )
  }

}

object MinMaxExprCodeGen {

  def min(expr: MinExpr[_]): MinMaxExprCodeGen = {
    new MinMaxExprCodeGen {
      override def expression: AggregateExpr[_, _, _] = expr

      override def zeroTree(innerExprValue: ValueDeclaration): Tree = q"${innerExprValue.valueName}"

      override def foldTree(accValue: ValueDeclaration, innerExprValue: ValueDeclaration): Tree =
        q"${CommonGen.ordValName(expression.expr.dataType)}.min(${accValue.valueName}, ${innerExprValue.valueName})"

      override def combineTree(accValueA: ValueDeclaration, accValueB: ValueDeclaration): Tree =
        q"${CommonGen.ordValName(expression.expr.dataType)}.min(${accValueA.valueName}, ${accValueB.valueName})"
    }
  }

  def max(expr: MaxExpr[_]): MinMaxExprCodeGen = {
    new MinMaxExprCodeGen {
      override def expression: AggregateExpr[_, _, _] = expr

      override def zeroTree(innerExprValue: ValueDeclaration): Tree = q"${innerExprValue.valueName}"

      override def foldTree(accValue: ValueDeclaration, innerExprValue: ValueDeclaration): Tree =
        q"${CommonGen.ordValName(expression.expr.dataType)}.max(${accValue.valueName}, ${innerExprValue.valueName})"

      override def combineTree(accValueA: ValueDeclaration, accValueB: ValueDeclaration): Tree =
        q"${CommonGen.ordValName(expression.expr.dataType)}.max(${accValueA.valueName}, ${accValueB.valueName})"
    }
  }
}
