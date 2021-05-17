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
import org.yupana.core.model.{ MetricData, QueryStates }

class PersistentMetricQueryCollectorWithExporter(
    collectorContext: QueryCollectorContext,
    query: Query,
    exporterCallBack: => Option[ExporterMetrics => Unit] = None
) extends PersistentMetricQueryCollector(collectorContext, query) {

  override def saveQueryMetrics(state: QueryStates.QueryState): MetricsResult = {
    val metrics = super.saveQueryMetrics(state)
    exporterCallBack.foreach(_(metricsResultToLabels(metrics)))
    metrics
  }

  private def explodeMetricData(prefix: String, metricData: MetricData): Seq[(String, String)] = {
    Seq(
      s"${prefix}_count" -> metricData.count.toString,
      s"${prefix}_time" -> metricData.time.toString,
      s"${prefix}_speed" -> metricData.speed.toString
    )
  }

  private def metricsResultToLabels(metricsResult: MetricsResult): ExporterMetrics = {
    val duration = metricsResult.durationSec
    val labels = Map(
      "id" -> metricsResult.queryId,
      "state" -> metricsResult.state,
      "is_spark" -> metricsResult.isSparkContext.toString
    ) ++
      metricsResult.metricsData.flatMap { case (pref, mData) => explodeMetricData(pref, mData) }
    ExporterMetrics(labels, duration)
  }

}

case class ExporterMetrics(labels: Map[String, String], durationSec: Double)
