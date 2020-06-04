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

import org.yupana.core.model.QueryStates.QueryState

trait MetricQueryCollector extends Serializable {

  def queryId: String

  def dynamicMetric(name: String): Metric

  def finish(): Unit

  def isEnabled: Boolean

  def saveQueryMetrics(state: QueryState): Unit
  def setRunningPartitions(partitions: Int): Unit
  def finishPartition(): Unit

  val createDimensionFilters: Metric = NoMetric
  val createScans: Metric = NoMetric
  val filterRows: Metric = NoMetric
  val windowFunctions: Metric = NoMetric
  val reduceOperation: Metric = NoMetric
  val postFilter: Metric = NoMetric
  val collectResultRows: Metric = NoMetric
  val dimensionValuesForIds: Metric = NoMetric
  val extractDataComputation: Metric = NoMetric
  val readExternalLinks: Metric = NoMetric
  val scan: Metric = NoMetric
  val parseScanResult: Metric = NoMetric
  val dictionaryScan: Metric = NoMetric
}

object NoMetricCollector extends MetricQueryCollector {

  override def dynamicMetric(name: String): Metric = NoMetric

  override def finish(): Unit = {}

  override def saveQueryMetrics(state: QueryState): Unit = {}

  override def setRunningPartitions(partitions: Int): Unit = {}

  override def finishPartition(): Unit = {}

  override val queryId: String = ""

  override val isEnabled: Boolean = false
}

trait Metric extends Serializable {
  def measure[T](count: Int)(f: => T): T
}

object NoMetric extends Metric {

  @inline
  override def measure[T](count: Int)(f: => T): T = f
}
