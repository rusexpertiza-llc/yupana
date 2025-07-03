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

object ProjectionStageGen {
  def mkProjection(
      query: Query,
      row: TermName,
      state: State
  ): (Seq[Tree], State) = {

    val aggregates = CommonGen.findAggregates(query.fields)
    val exprs = if (aggregates.isEmpty) {
      query.fields
        .map(_.expr)
        .filter(ExpressionCodeGenFactory.needEvaluateInProjectionStage)
        .toList
    } else {
      query.groupBy.filter(ExpressionCodeGenFactory.needEvaluateForGroupBy)
    }

    val stateWithExtLinkExprs = findExternalLinkExprs(query).foldLeft(state) { (s, expr) =>
      expr.link.requiredExpressions.foldLeft(s) { (s, e) =>
        s.withExpression(e)
      }
    }

    val stateWithExprs = query.fields
      .map(_.expr)
      .filterNot(ExpressionCodeGenFactory.needEvaluateInProjectionStage)
      .foldLeft(stateWithExtLinkExprs) { (s, expr) =>
        s.withExpression(expr)
      }

    val (ts, ns) = exprs.foldLeft((Seq.empty[Tree], stateWithExprs)) {
      case ((ts, s), e) =>
        val r = ExpressionCodeGenFactory.codeGenerator(e).generateEvalCode(s, row)
        val s2 = r.state.withWriteToRow(row, e, r.valueDeclaration)
        (ts ++ r.trees) -> s2
    }
    ts -> ns
  }

  private def findExternalLinkExprs(query: Query): Seq[LinkExpr[_]] = {
    val allExprs = query.groupBy ++ query.fields.map(_.expr) ++ query.filter.toList ++ query.postFilter.toList
    allExprs.flatMap(findExternalLinkExprs)
  }

  private def findExternalLinkExprs(expr: Expression[_]): Seq[LinkExpr[_]] = {
    expr.fold(Seq.empty[LinkExpr[_]]) {
      case (links, expr: LinkExpr[_]) => links :+ expr
      case (links, _)                 => links
    }
  }
}
