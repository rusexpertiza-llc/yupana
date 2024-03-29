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

import org.yupana.core.model.TsdbQueryMetrics
import org.yupana.core.utils.metric.InternalMetricData
import org.yupana.metrics.QueryStates

trait TsdbQueryMetricsDao {

  def queriesByFilter(filter: Option[QueryMetricsFilter], limit: Option[Int]): Iterator[TsdbQueryMetrics]

  def saveQueryMetrics(metrics: List[InternalMetricData]): Unit

  def deleteMetrics(filter: QueryMetricsFilter): Int
}

case class QueryMetricsFilter(
    queryId: Option[String] = None,
    queryState: Option[QueryStates.QueryState] = None
)
