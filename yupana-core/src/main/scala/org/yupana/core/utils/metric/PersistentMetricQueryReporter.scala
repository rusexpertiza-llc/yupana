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
import org.yupana.metrics.{ MetricReporter, QueryStates }

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.{ Timer, TimerTask }
import scala.collection.mutable

class PersistentMetricQueryReporter(metricsDao: TsdbQueryMetricsDao, asyncSaving: Boolean = true)
    extends MetricReporter[MetricQueryCollector] {

  private val UPDATE_INTERVAL = 60 * 1000L
  private val asyncBuffer = new ConcurrentLinkedQueue[InternalMetricData]
  private val saveTimer = new Timer(true)

  if (asyncSaving) {
    saveTimer.scheduleAtFixedRate(
      new TimerTask {
        def run(): Unit = {
          saveMetricsFromBuffer()
        }
      },
      0L,
      UPDATE_INTERVAL
    )
  }

  private def saveMetricsFromBuffer(): Unit = {
    if (!asyncBuffer.isEmpty) {
      val metricsToSave = mutable.ListBuffer.empty[InternalMetricData]
      while (!asyncBuffer.isEmpty) {
        val m = asyncBuffer.poll()
        if (m != null) metricsToSave += m
      }
      metricsDao.saveQueryMetrics(metricsToSave.toList)
    }
  }

  override def start(mc: MetricQueryCollector, partitionId: Option[String]): Unit = {
    metricsDao.saveQueryMetrics(List(InternalMetricData.fromMetricCollector(mc, partitionId, QueryStates.Running)))
  }

  def saveQueryMetrics(mc: MetricQueryCollector, partitionId: Option[String], state: QueryStates.QueryState): Unit = {
    val metricsData = InternalMetricData.fromMetricCollector(mc, partitionId, state)
    if (asyncSaving) {
      asyncBuffer.add(metricsData)
    } else {
      metricsDao.saveQueryMetrics(List(metricsData))
    }
  }

  override def finish(mc: MetricQueryCollector, partitionId: Option[String]): Unit = {
    saveMetricsFromBuffer()
  }
}
