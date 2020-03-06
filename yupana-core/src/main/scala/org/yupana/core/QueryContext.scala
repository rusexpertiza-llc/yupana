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

package org.yupana.core

import com.typesafe.scalalogging.StrictLogging
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._

import scala.collection.mutable

case class QueryContext(
    query: Query,
    postCondition: Option[Condition],
    exprsIndex: mutable.HashMap[Expression, Int],
    aggregateExprs: Array[AggregateExpr],
    topRowExprs: Array[Expression],
    exprsOnAggregatesAndWindows: Array[Expression],
    bottomExprs: Array[Expression],
    linkExprs: Seq[LinkExpr[_]],
    groupByExprs: Array[Expression]
)

object QueryContext extends StrictLogging {

  def apply(query: Query, postCondition: Option[Condition]): QueryContext = {
    import org.yupana.core.utils.QueryUtils.{ requiredDimensions, requiredLinks }

    val requiredTags = query.groupBy.flatMap(requiredDimensions).toSet ++
      query.fields.flatMap(f => requiredDimensions(f.expr)).toSet ++
      postCondition.toSet.flatMap(requiredDimensions) ++
      query.postFilter.toSeq.flatMap(requiredDimensions)

    val requiredDimExprs = requiredTags.map(DimensionExpr(_))

    val groupByExternalLinks = query.groupBy.flatMap(requiredLinks)
    val fieldsExternalLinks = query.fields.flatMap(f => requiredLinks(f.expr))
    val dataFiltersExternalLinks = postCondition.toSet.flatMap(requiredLinks)
    val havingExternalLinks = query.postFilter.toSeq.flatMap(requiredLinks)

    val linkExprs =
      (groupByExternalLinks ++ fieldsExternalLinks ++ dataFiltersExternalLinks ++ havingExternalLinks).distinct

    val topExprs: Set[Expression] = query.fields.map(_.expr).toSet ++
      (query.groupBy.toSet ++
        requiredDimExprs ++
        query.postFilter.toSet ++
        postCondition.toSet +
        TimeExpr).filterNot(_.isInstanceOf[ConstantExpr])

    val topRowExprs: Set[Expression] = topExprs.filter { expr =>
      !expr.isInstanceOf[ConstantExpr] && (
        (!containsAggregates(expr) && !containsWindows(expr)) ||
        expr.isInstanceOf[AggregateExpr] ||
        expr.isInstanceOf[WindowFunctionExpr]
      )
    }

    val exprsOnAggregatesAndWindows = topExprs diff topRowExprs

    val allExprs: Set[Expression] = topExprs.flatMap(_.flatten)

    val bottomExprs: Set[Expression] = collectBottomExprs(allExprs)

    val aggregateExprs = allExprs.collect { case ae: AggregateExpr => ae }

    val exprsIndex = mutable.HashMap(allExprs.zipWithIndex.toSeq: _*)

    new QueryContext(
      query,
      postCondition.filterNot(_ == ConstantExpr(true)),
      exprsIndex,
      aggregateExprs.toArray,
      (topRowExprs -- bottomExprs).toArray,
      exprsOnAggregatesAndWindows.toArray,
      bottomExprs.toArray,
      linkExprs,
      query.groupBy.toArray
    )
  }

  private def collectBottomExprs(exprs: Set[Expression]): Set[Expression] = {
    exprs.collect {
      case a @ AggregateExpr(_, e)        => Set(a, e)
      case ConditionExpr(condition, _, _) => Set(condition)
      case c: ConstantExpr                => Set(c)
      case t: DimensionExpr               => Set(t)
      case c: LinkExpr[_]                 => Set(c)
      case v: MetricExpr[_]               => Set(v)
      case TimeExpr                       => Set(TimeExpr)
      case _                              => Set.empty
    }.flatten
  }

  private def containsAggregates(e: Expression): Boolean = e.flatten.exists {
    case _: AggregateExpr => true
    case _                => false
  }

  private def containsWindows(e: Expression): Boolean = e.flatten.exists {
    case _: WindowFunctionExpr => true
    case _                     => false
  }
}
