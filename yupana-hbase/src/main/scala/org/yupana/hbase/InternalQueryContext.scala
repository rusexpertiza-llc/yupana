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
    tagFields: Array[Option[Either[Metric, Dimension]]],
    dimIndexMap: mutable.Map[Dimension, Int],
    requiredDims: Set[Dimension],
    metricsCollector: MetricQueryCollector
) {

  private val exprsTags: Array[Byte] = {
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

  private val tagExprsIndexes = {
    val arr = Array.ofDim[Array[Int]](Table.MAX_TAGS)
    exprsTags.zipWithIndex.groupBy(_._1).mapValues(_.map(_._2)).foreach {
      case (tag, exprIndexes) =>
        arr(tag & 0xFF) = exprIndexes
    }
    arr
  }

  @inline
  def tagForExprIndex(index: Int): Byte = exprsTags(index)

  @inline
  final def fieldForTag(tag: Byte): Option[Either[Metric, Dimension]] = tagFields(tag & 0xFF)

  def exprIndexesForTag(tag: Byte): Array[Int] = {
    tagExprsIndexes(tag & 0xFF)
  }
}

object InternalQueryContext {
  def apply(query: InternalQuery, metricCollector: MetricQueryCollector): InternalQueryContext = {
    val tagFields = Array.fill[Option[Either[Metric, Dimension]]](255)(None)

    query.table.metrics.foreach { m =>
      tagFields(m.tag & 0xFF) = Some(Left(m))
    }

    query.table.dimensionSeq.foreach { dim =>
      tagFields(query.table.dimensionTag(dim) & 0xFF) = Some(Right(dim))
    }

    val requiredDims: Set[Dimension] = query.exprs.collect {
      case DimensionExpr(dim) => dim
    }

    val dimIndexMap = mutable.HashMap(query.table.dimensionSeq.zipWithIndex: _*)

    val exprsIndexSeq = query.exprs.toSeq.zipWithIndex

    new InternalQueryContext(query.table, exprsIndexSeq, tagFields, dimIndexMap, requiredDims, metricCollector)
  }
}
