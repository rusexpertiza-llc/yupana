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
  def queriesByFilter(filter: QueryMetricsFilter): Iterable[TsdbQueryMetrics]
  def updateQueryMetrics(
      queryId: String,
      queryState: QueryState,
      totalDuration: Double,
      metricValues: Map[String, MetricData],
      sparkQuery: Boolean
  ): Unit
  def setQueryState(queryId: String, queryState: QueryState): Boolean
}

case class QueryMetricsFilter(queryId: Option[String], limit: Int = 1)
