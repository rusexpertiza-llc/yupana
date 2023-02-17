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

import org.yupana.core.dao.TsdbQueryMetricsDao
import org.yupana.core.model.QueryStates.QueryState
import org.yupana.core.model.{ MetricData, QueryStates }
import org.yupana.metrics.{ MetricCollector, MetricReporter }

class PersistentMetricQueryReporter(metricsDao: () => TsdbQueryMetricsDao)
    extends MetricReporter[MetricQueryCollector, QueryState] {

  override def start(mc: MetricQueryCollector, partitionId: Option[String]): Unit = {
    metricsDao().saveQueryMetrics(
      mc.query,
      partitionId,
      mc.startTime,
      QueryStates.Running,
      0,
      Map.empty,
      mc.isSparkQuery
    )
  }

  private def createMetricsData(mc: MetricQueryCollector): Map[String, MetricData] = {
    mc.allMetrics.map { m =>
      val cnt = m.count
      val time = MetricCollector.asSeconds(m.time)
      val speed = if (time != 0) cnt.toDouble / time else 0.0
      val data = MetricData(cnt, m.time, speed)
      m.name -> data
    }.toMap
  }

  def saveQueryMetrics(mc: MetricQueryCollector, partitionId: Option[String], state: QueryState): Unit = {
    val metricsData = createMetricsData(mc)
    metricsDao().saveQueryMetrics(
      mc.query,
      partitionId,
      mc.startTime,
      state,
      mc.resultDuration,
      metricsData,
      mc.isSparkQuery
    )
  }

  override def finish(mc: MetricQueryCollector, partitionId: Option[String]): Unit = {}
}
