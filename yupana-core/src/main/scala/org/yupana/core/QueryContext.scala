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
        postCondition.toSet ++
        query.table.map(_ => TimeExpr)).filterNot(_.isInstanceOf[ConstantExpr[_]])

    val allExprs: Set[Expression[_]] = topExprs.flatMap(e => e.flatten.filterNot(_.isInstanceOf[ConstantExpr[_]]) + e)

    val exprsIndex = mutable.HashMap(allExprs.zipWithIndex.toSeq: _*)

    new QueryContext(
      query,
      exprsIndex,
      linkExprs,
      query.groupBy.toArray
    )
  }
}
