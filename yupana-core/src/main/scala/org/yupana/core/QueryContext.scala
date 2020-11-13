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
    exprsIndex: mutable.HashMap[Expression[_], Int],
    aggregateExprs: Array[AggregateExpr[_, _, _]],
    topRowExprs: Array[Expression[_]],
    exprsOnAggregatesAndWindows: Array[Expression[_]],
    bottomExprs: Array[Expression[_]],
    linkExprs: Seq[LinkExpr[_]],
    groupByExprs: Array[Expression[_]]
)

object QueryContext extends StrictLogging {

  def apply(query: Query, postCondition: Option[Condition]): QueryContext = {
    import org.yupana.core.utils.QueryUtils.{ requiredDimensions, requiredLinks }

    val requiredDims = query.groupBy.flatMap(requiredDimensions).toSet ++
      query.fields.flatMap(f => requiredDimensions(f.expr)).toSet ++
      postCondition.toSet.flatMap(requiredDimensions) ++
      query.postFilter.toSeq.flatMap(requiredDimensions)

    val requiredDimExprs = requiredDims.map(d => DimensionExpr(d.aux))

    val groupByExternalLinks = query.groupBy.flatMap(requiredLinks)
    val fieldsExternalLinks = query.fields.flatMap(f => requiredLinks(f.expr))
    val dataFiltersExternalLinks = postCondition.toSet.flatMap(requiredLinks)
    val havingExternalLinks = query.postFilter.toSeq.flatMap(requiredLinks)

    val linkExprs =
      (groupByExternalLinks ++ fieldsExternalLinks ++ dataFiltersExternalLinks ++ havingExternalLinks).distinct

    val topExprs: Set[Expression[_]] = query.fields.map(_.expr).toSet ++
      (query.groupBy.toSet ++
        requiredDimExprs ++
        query.postFilter.toSet ++
        postCondition.toSet +
        TimeExpr).filterNot(_.isInstanceOf[ConstantExpr[_]])

    val topRowExprs: Set[Expression[_]] = topExprs.filter { expr =>
      !expr.isInstanceOf[ConstantExpr[_]] && (
        (!containsAggregates(expr) && !containsWindows(expr)) ||
        expr.isInstanceOf[AggregateExpr[_, _, _]] ||
        expr.isInstanceOf[WindowFunctionExpr[_, _]]
      )
    }

    val exprsOnAggregatesAndWindows = topExprs diff topRowExprs

    val allExprs: Set[Expression[_]] = topExprs.flatMap(_.flatten)

    val bottomExprs: Set[Expression[_]] = collectBottomExprs(allExprs)

    val aggregateExprs = allExprs.collect { case ae: AggregateExpr[_, _, _] => ae }

    val exprsIndex = mutable.HashMap(allExprs.zipWithIndex.toSeq: _*)

    new QueryContext(
      query,
      exprsIndex,
      aggregateExprs.toArray,
      (topRowExprs -- bottomExprs).toArray,
      exprsOnAggregatesAndWindows.toArray,
      bottomExprs.toArray,
      linkExprs,
      query.groupBy.toArray
    )
  }

  private def collectBottomExprs(exprs: Set[Expression[_]]): Set[Expression[_]] = {
    exprs.collect {
      case a: AggregateExpr[_, _, _]      => Set(a, a.expr)
      case ConditionExpr(condition, _, _) => Set(condition)
      case c: ConstantExpr[_]             => Set(c)
      case d: DimensionExpr[_]            => Set(d)
      case i: DimensionIdExpr             => Set(i)
      case c: LinkExpr[_]                 => Set(c)
      case m: MetricExpr[_]               => Set(m)
      case TimeExpr                       => Set(TimeExpr)
      case _                              => Set.empty
    }.flatten
  }

  private def containsAggregates(e: Expression[_]): Boolean = e.flatten.exists {
    case _: AggregateExpr[_, _, _] => true
    case _                         => false
  }

  private def containsWindows(e: Expression[_]): Boolean = e.flatten.exists {
    case _: WindowFunctionExpr[_, _] => true
    case _                           => false
  }
}
