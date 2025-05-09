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
import org.yupana.core.jit.{ ExpressionCodeGenFactory, State }

import scala.reflect.runtime.universe._

object PostCombineStageGen {
  def mkPostCombine(
      state: State,
      query: Query,
      row: TermName
  ): (Seq[Tree], State) = {

    val (aggTrees, aggState) = CommonGen.findAggregates(query.fields).foldLeft((Seq.empty[Tree], state)) {
      case ((ts, s), ae) =>
        val r = ExpressionCodeGenFactory
          .aggExprCodeGenerator(ae)
          .generatePostCombineCode(s, row)
        (ts ++ r.trees) -> r.state
    }

    if (aggState.hasWriteOps) {
      val groupByState = CommonGen.copyGroupByFields(aggState, query, row)
      (aggTrees, groupByState)
    } else {
      (aggTrees, aggState)
    }
  }
}
