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

package org.yupana.akka

import org.yupana.core.TSDB
import org.yupana.api.Time
import org.yupana.api.query.{ Result, SimpleResult }
import org.yupana.api.types.DataType
import org.yupana.core.dao.QueryMetricsFilter
import org.yupana.core.model.QueryStates

object QueryInfoProvider {

  def handleShowQueries(tsdb: TSDB, queryId: Option[String], limit: Int = 20): Result = {
    import org.yupana.core.model.TsdbQueryMetrics._

    val queries = tsdb.metricsDao.queriesByFilter(
      QueryMetricsFilter(
        limit = limit,
        queryId = queryId
      )
    )
    val data: Iterator[Array[Option[Any]]] = queries.map { query =>
      Array[Option[Any]](
        Some(query.queryId),
        Some(query.state.name),
        Some(query.query),
        Some(Time(query.startDate)),
        Some(query.totalDuration)
      ) ++ qualifiers.flatMap { q =>
        val metric = query.metrics(q)
        Array(Some(metric.count.toDouble), Some(metric.time), Some(metric.speed))
      }
    }.iterator

    val queryFieldNames = List(queryIdColumn, stateColumn, queryColumn, startDateColumn, totalDurationColumn) ++
      qualifiers.flatMap(q => List(q + "_" + metricCount, q + "_" + metricTime, q + "_" + metricSpeed))

    val queryFieldTypes = List(DataType[String], DataType[String], DataType[String], DataType[Time], DataType[Double]) ++
      (0 until qualifiers.size * 3).map(_ => DataType[Double])

    SimpleResult("QUERIES", queryFieldNames, queryFieldTypes, data)
  }

  def handleKillQuery(tsdb: TSDB, id: String): Result = {
    val result = if (tsdb.metricsDao.setQueryState(id, QueryStates.Cancelled)) {
      "OK"
    } else {
      "QUERY NOT FOUND"
    }
    SimpleResult("RESULT", List("RESULT"), List(DataType[String]), Iterator(Array(Some(result))))
  }
}
