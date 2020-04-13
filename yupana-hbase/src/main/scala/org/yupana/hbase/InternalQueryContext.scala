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

package org.yupana.hbase

import org.yupana.api.query.{ DimensionExpr, Expression, MetricExpr }
import org.yupana.core.model.InternalQuery
import org.yupana.api.schema.{ Dimension, Metric, Table }
import org.yupana.core.utils.metric.MetricQueryCollector

import scala.collection.mutable

case class InternalQueryContext(
    table: Table,
    exprsIndexSeq: Seq[(Expression, Int)],
    fieldIndexMap: Array[Option[Either[Metric, Dimension]]],
    requiredDims: Set[Dimension],
    metricsCollector: MetricQueryCollector
) {

  val exprsTags: Array[Byte] = {
    exprsIndexSeq.map {
      case (expr, index) =>
        expr match {
          case MetricExpr(metric) =>
            metric.tag
          case DimensionExpr(dimension: Dimension) =>
            table.dimensionTag(dimension)
          case _ => 0.toByte
        }
    }.toArray
  }

  @inline
  def tagForExprIndex(index: Int) = exprsTags(index)
}

object InternalQueryContext {
  def apply(query: InternalQuery, metricCollector: MetricQueryCollector): InternalQueryContext = {
    val fieldIndexMap = Array.fill[Option[Either[Metric, Dimension]]](255)(None)

    query.table.metrics.foreach { m =>
      fieldIndexMap(m.tag) = Some(Left(m))
    }

    query.table.dimensionSeq.zipWithIndex.foreach {
      case (dim, idx) =>
        fieldIndexMap(Table.DIM_TAG_OFFSET + idx) = Some(Right(dim))
    }

    val requiredDims = query.exprs.collect {
      case DimensionExpr(dim) => dim
    }

    val exprsIndexSeq = query.exprs.toSeq.zipWithIndex

    new InternalQueryContext(query.table, exprsIndexSeq, fieldIndexMap, requiredDims, metricCollector)
  }
}
