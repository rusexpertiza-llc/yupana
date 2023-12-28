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
import org.yupana.core.model.MetricData
import org.yupana.metrics.{ MetricCollector, MetricReporter, QueryStates }

import java.util.{ Timer, TimerTask }
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.mutable

class PersistentMetricQueryReporter(metricsDao: () => TsdbQueryMetricsDao, asyncSaving: Boolean = true)
    extends MetricReporter[MetricQueryCollector] {

  private val UPDATE_INTERVAL = 60 * 1000L
  private val asyncBuffer = new ConcurrentLinkedQueue[InternalMetricData]
  private val saveTimer = new Timer(true)

  if (asyncSaving) {
    saveTimer.scheduleAtFixedRate(
      new TimerTask {
        def run(): Unit = {
          saveMetricsBuffer()
        }
      },
      0L,
      UPDATE_INTERVAL
    )
  }

  private def saveMetricsBuffer(): Unit = {
    if (asyncBuffer.size() > 0) {
      val metricsToSave = mutable.ListBuffer.empty[InternalMetricData]
      while (asyncBuffer.size() > 0) {
        metricsToSave += asyncBuffer.poll()
      }
      metricsDao().saveQueryMetrics(metricsToSave.toList)
    }
  }

  override def start(mc: MetricQueryCollector, partitionId: Option[String]): Unit = {}

  private def createMetricsData(mc: MetricQueryCollector): Map[String, MetricData] = {
    mc.allMetrics.map { m =>
      val cnt = m.count
      val time = MetricCollector.asSeconds(m.time)
      val speed = if (time != 0) cnt.toDouble / time else 0.0
      val data = MetricData(cnt, m.time, speed)
      m.name -> data
    }.toMap
  }

  def saveQueryMetrics(mc: MetricQueryCollector, partitionId: Option[String], state: QueryStates.QueryState): Unit = {
    val metricsData = createMetricsData(mc)
    asyncBuffer.add(
      InternalMetricData(
        mc.query,
        partitionId,
        mc.startTime,
        state,
        mc.resultDuration,
        metricsData,
        mc.isSparkQuery
      )
    )
    if (!asyncSaving) {
      saveMetricsBuffer()
    }
  }

  override def finish(mc: MetricQueryCollector, partitionId: Option[String]): Unit = {
    saveTimer.cancel()
  }
}
