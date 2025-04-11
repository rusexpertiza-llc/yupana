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
import org.yupana.metrics.{ Metric, NoMetric }

object NoMetricCollector extends MetricQueryCollector {

  override def initQueryContext: Metric = NoMetric
  override val createDimensionFilters: Metric = NoMetric
  override val createScans: Metric = NoMetric
  override val scan: Metric = NoMetric
  override val createContext: Metric = NoMetric
  override val readExternalLinks: Metric = NoMetric
  override val extractDataComputation: Metric = NoMetric
  override val filterRows: Metric = NoMetric
  override val filter: Metric = NoMetric
  override val evaluateExpressions: Metric = NoMetric
  override val extractKeyData: Metric = NoMetric
  override val windowFunctions: Metric = NoMetric
  override val reduceOperation: Metric = NoMetric
  override val postFilter: Metric = NoMetric
  override val collectResultRows: Metric = NoMetric

  override def dynamicMetric(name: String): Metric = NoMetric

  override val partitionId: Option[String] = None

  override def start(): Unit = {}
  override def checkpoint(): Unit = {}
  override def metricUpdated(metric: Metric, time: Long): Unit = {}
  override def finish(): Unit = {}

  override val query: Query = null

  override val user: String = "NO USER"

  override val isEnabled: Boolean = false
  override def isSparkQuery: Boolean = false

  override val allMetrics: Seq[Metric] = Seq.empty

  override val operationName: String = "UNKNOWN"
  override def startTime: Long = 0L
  override def resultDuration: Long = 0L
}
