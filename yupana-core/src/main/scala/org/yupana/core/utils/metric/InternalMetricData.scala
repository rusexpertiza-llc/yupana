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

package org.yupana.core.utils.metric

import org.yupana.api.query.Query
import org.yupana.core.model.MetricData
import org.yupana.metrics.{ MetricCollector, QueryStates }

case class InternalMetricData(
    query: Query,
    partitionId: Option[String],
    startDate: Long,
    queryState: QueryStates.QueryState,
    totalDuration: Long,
    metricValues: Map[String, MetricData],
    sparkQuery: Boolean,
    user: String
)

object InternalMetricData {
  def fromMetricCollector(
      mc: MetricQueryCollector,
      partitionId: Option[String],
      state: QueryStates.QueryState
  ): InternalMetricData = {
    val data = mc.allMetrics.map { m =>
      val cnt = m.count
      val time = MetricCollector.asSeconds(m.time)
      val speed = if (time != 0) cnt.toDouble / time else 0.0
      val data = MetricData(cnt, m.time, speed)
      m.name -> data
    }.toMap

    InternalMetricData(
      mc.query,
      partitionId,
      mc.startTime,
      state,
      mc.resultDuration,
      data,
      mc.isSparkQuery,
      mc.user
    )
  }
}
