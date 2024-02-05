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

package org.yupana.core.providers

import org.yupana.api.Time
import org.yupana.api.query.{ Result, SimpleResult }
import org.yupana.api.types.DataType
import org.yupana.core.FlatQueryEngine
import org.yupana.core.dao.QueryMetricsFilter
import org.yupana.core.sql.parser.MetricsFilter
import org.yupana.metrics.{ MetricCollector, QueryStates }

object QueryInfoProvider {

  private def getFilter(sqlFilter: MetricsFilter): QueryMetricsFilter = {
    QueryMetricsFilter(
      queryId = sqlFilter.queryId,
      queryState = sqlFilter.state.map(s => QueryStates.getByName(s))
    )
  }

  def handleShowQueries(
      flatQueryEngine: FlatQueryEngine,
      sqlFilter: Option[MetricsFilter],
      limit: Option[Int]
  ): Result = {
    import org.yupana.core.model.TsdbQueryMetrics._

    val filter = sqlFilter.map(getFilter)
    val metrics = flatQueryEngine.queriesByFilter(filter, limit)
    val data: Iterator[Array[Any]] = metrics.map { queryMetrics =>
      Array[Any](
        queryMetrics.queryId,
        queryMetrics.engine,
        queryMetrics.state.name,
        queryMetrics.query,
        Time(queryMetrics.startDate),
        MetricCollector.asSeconds(queryMetrics.totalDuration)
      ) ++ qualifiers.flatMap { q =>
        queryMetrics.metrics.get(q) match {
          case Some(metric) =>
            Array(metric.count.toString, MetricCollector.asSeconds(metric.time).toString, metric.speed.toString)
          case None =>
            Array("-", "-", "-")
        }
      }
    }

    val queryFieldNames = List(
      queryIdColumn,
      engineColumn,
      stateColumn,
      queryColumn,
      startDateColumn,
      totalDurationColumn
    ) ++
      qualifiers.flatMap(q => List(q + "_" + metricCount, q + "_" + metricTime, q + "_" + metricSpeed))

    val queryFieldTypes = List(
      DataType[String],
      DataType[String],
      DataType[String],
      DataType[String],
      DataType[Time],
      DataType[Double]
    ) ++
      (0 until qualifiers.size * 3).map(_ => DataType[String])

    SimpleResult("QUERIES", queryFieldNames, queryFieldTypes, data)
  }

  def handleKillQuery(flatQueryEngine: FlatQueryEngine, sqlFilter: MetricsFilter): Result = {
    flatQueryEngine.setQueryState(getFilter(sqlFilter), QueryStates.Cancelled)
    SimpleResult("RESULT", List("RESULT"), List(DataType[String]), Iterator(Array[Any]("OK")))
  }

  def handleDeleteQueryMetrics(flatQueryEngine: FlatQueryEngine, sqlFilter: MetricsFilter): Result = {
    val deleted = flatQueryEngine.deleteMetrics(getFilter(sqlFilter))
    SimpleResult("RESULT", List("DELETED"), List(DataType[Int]), Iterator(Array[Any](deleted)))
  }
}
