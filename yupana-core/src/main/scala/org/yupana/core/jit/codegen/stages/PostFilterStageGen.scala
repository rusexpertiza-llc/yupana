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

object PostFilterStageGen {
  def mkPostFilter(state: State, row: TermName, query: Query): (Seq[Tree], State) = {
    val aggregatesAndWinFuncs = CommonGen.findAggregates(query.fields) ++ CommonGen.findWindowFunctions(query.fields)
    val readExprsState = aggregatesAndWinFuncs.zipWithIndex.foldLeft(state) {
      case (s, (e, idx)) =>
        s.withReadFromRow(row, e, ValueDeclaration(s"agg_$idx"))
    }

    query.postFilter match {
      case None       => Seq(q"true") -> readExprsState
      case Some(cond) =>
        val res = ExpressionCodeGenFactory.codeGenerator(cond).generateEvalCode(readExprsState, row)
        val tree = q"${res.valueDeclaration.validityFlagName} && ${res.valueDeclaration.valueName}"
        (res.trees :+ tree) -> res.state
    }
  }
}
