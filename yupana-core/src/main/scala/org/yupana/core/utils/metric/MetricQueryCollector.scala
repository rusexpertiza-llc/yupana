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

trait MetricQueryCollector extends Serializable {

  def query: Query
  def operationName: String

  def dynamicMetric(name: String): Metric

  def isEnabled: Boolean

  def finish(): Unit

  def metricUpdated(metric: Metric, time: Long): Unit
  def setRunningPartitions(partitions: Int): Unit
  def finishPartition(): Unit

  def allMetrics: Seq[Metric]

  def startTime: Long
  def resultTime: Long
}

object NoMetricCollector extends MetricQueryCollector {

  override def dynamicMetric(name: String): Metric = NoMetric

  override def operationName: String = "UNKNOWN"

  override def finish(): Unit = {}

  override def metricUpdated(metric: Metric, time: Long): Unit = {}

  override def setRunningPartitions(partitions: Int): Unit = {}

  override def finishPartition(): Unit = {}

  override val query: Query = null

  override val isEnabled: Boolean = false

  override val allMetrics: Seq[Metric] = Seq.empty

  override def startTime: Long = 0L
  override def resultTime: Long = 0L
}

object NoMetric extends Metric {
  override val name: String = "NONE"

  override def time: Long = 0L
  override def count: Long = 0L

  @inline
  override def measure[T](count: Int)(f: => T): T = f
}
