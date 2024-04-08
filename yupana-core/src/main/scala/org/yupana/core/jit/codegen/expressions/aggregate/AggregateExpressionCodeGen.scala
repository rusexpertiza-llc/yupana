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
import org.yupana.core.jit.codegen.expressions.ExpressionCodeGen
import org.yupana.core.jit.{ CodeGenResult, State }

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

trait AggregateExpressionCodeGen[T <: AggregateExpr[_, _, _]] extends ExpressionCodeGen[T] {

  def generateZeroCode(state: State, accRow: TermName, row: TermName): CodeGenResult

  def generateFoldCode(state: State, accRow: TermName, row: TermName): CodeGenResult

  def generateCombineCode(state: State, accRowA: TermName, accRowB: TermName): CodeGenResult

  def generatePostCombineCode(state: State, acc: TermName): CodeGenResult

  override def generateEvalCode(state: State, row: universe.TermName): CodeGenResult = {
    state.rowOperations.filter(_.opType == State.RowOpType.ReadField).find { op =>
      op.expr == expression
    } match {
      case Some(op) =>
        CodeGenResult(Seq.empty, op.valueDeclaration, state)
      case None =>
        throw new IllegalStateException("Something went wrong")
    }
  }
}
