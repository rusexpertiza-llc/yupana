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

class PersistentMetricQueryReporter(metricsDao: () => TsdbQueryMetricsDao) extends MetricReporter {

  override def start(mc: MetricQueryCollector): Unit = {
    metricsDao().initializeQueryMetrics(mc.query, mc.isSparkQuery)
  }

  private def getAndResetMetricsData(mc: MetricQueryCollector): Map[String, MetricData] = {
    mc.allMetrics.map { m =>
      val cnt = m.count
      val time = StandardMetricCollector.asSeconds(m.time)
      val speed = if (time != 0) cnt.toDouble / time else 0.0
      val data = MetricData(cnt, time, speed)
      m.name -> data
    }.toMap
  }

  def saveQueryMetrics(mc: MetricQueryCollector, state: QueryState): Unit = {
    val duration = StandardMetricCollector.asSeconds(mc.resultTime)
    val metricsData = getAndResetMetricsData(mc)

    metricsDao().updateQueryMetrics(mc.query.id, state, duration, metricsData, mc.isSparkQuery)
  }

  override def finish(mc: MetricQueryCollector): Unit = {}

  override def setRunningPartitions(mc: MetricQueryCollector, partitions: Int): Unit = {
    metricsDao().setRunningPartitions(mc.query.id, partitions)
  }

  override def finishPartition(mc: MetricQueryCollector): Unit = {
    val restPartitions = metricsDao().decrementRunningPartitions(mc.query.id)
    saveQueryMetrics(mc, QueryStates.Running)

    if (restPartitions <= 0) {
      finish(mc)
    }
  }
}
