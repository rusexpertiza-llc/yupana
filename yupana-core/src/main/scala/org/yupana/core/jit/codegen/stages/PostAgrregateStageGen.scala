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

package org.yupana.core.jit.codegen.stages

import org.yupana.api.query._
import org.yupana.core.jit.codegen.CommonGen
import org.yupana.core.jit.{ ExpressionCodeGenFactory, State, ValueDeclaration }

import scala.reflect.runtime.universe._
object PostAgrregateStageGen {
  def mkPostAggregate(
      query: Query,
      row: TermName,
      state: State
  ): (Seq[Tree], State) = {
    val aggregatesAndWinFuncs = CommonGen.findAggregates(query.fields) ++ CommonGen.findWindowFunctions(query.fields)

    if (aggregatesAndWinFuncs.nonEmpty || query.fields.exists(_.expr.isInstanceOf[ConstantExpr[_]])) {

      val readExprsState = aggregatesAndWinFuncs.zipWithIndex.foldLeft(state) {
        case (s, (e, idx)) =>
          s.withReadFromRow(row, e, ValueDeclaration(s"agg_$idx"))
      }

      query.fields
        .map(_.expr)
        .filterNot(query.groupBy.contains)
        .foldLeft((Seq.empty[Tree], readExprsState)) {
          case ((ts, s), e) =>
            val r = ExpressionCodeGenFactory.codeGenerator(e).generateEvalCode(s, row)
            val s2 = r.state.withWriteToRow(row, e, r.valueDeclaration)
            (ts ++ r.trees) -> s2
        }
    } else {
      Seq.empty -> state
    }
  }
}
