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

package org.yupana.core.dao

import org.yupana.api.query.Query
import org.yupana.core.model.QueryStates.QueryState
import org.yupana.core.model.{ MetricData, TsdbQueryMetrics }

trait TsdbQueryMetricsDao {

  def initializeQueryMetrics(query: Query, sparkQuery: Boolean): Unit

  def queriesByFilter(filter: Option[QueryMetricsFilter], limit: Option[Int]): Iterable[TsdbQueryMetrics]

  def updateQueryMetrics(
      rowKey: Long,
      queryState: QueryState,
      totalDuration: Double,
      metricValues: Map[String, MetricData],
      sparkQuery: Boolean
  ): Unit

  def setQueryState(filter: QueryMetricsFilter, queryState: QueryState): Unit

  def setRunningPartitions(queryRowKey: Long, partitions: Int): Unit

  def decrementRunningPartitions(queryRowKey: Long): Int

  def deleteMetrics(filter: QueryMetricsFilter): Int
}

case class QueryMetricsFilter(
    queryId: Option[Long] = None,
    queryState: Option[QueryState] = None
)
