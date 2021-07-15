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

trait MetricReporter {

  def start(mc: MetricQueryCollector): Unit
  def finish(mc: MetricQueryCollector): Unit

  def saveQueryMetrics(mc: MetricQueryCollector, state: QueryState): Unit
  def setRunningPartitions(mc: MetricQueryCollector, partitions: Int): Unit
  def finishPartition(mc: MetricQueryCollector): Unit

}

class CombinedMetricReporter(reporters: MetricReporter*) extends MetricReporter {
  override def start(mc: MetricQueryCollector): Unit = reporters.foreach(_.start(mc))

  override def finish(mc: MetricQueryCollector): Unit = reporters.foreach(_.finish(mc))

  override def saveQueryMetrics(mc: MetricQueryCollector, state: QueryState): Unit =
    reporters.foreach(_.saveQueryMetrics(mc, state))

  override def setRunningPartitions(mc: MetricQueryCollector, partitions: Int): Unit =
    reporters.foreach(_.setRunningPartitions(mc, partitions))

  override def finishPartition(mc: MetricQueryCollector): Unit = reporters.foreach(_.finishPartition(mc))
}
