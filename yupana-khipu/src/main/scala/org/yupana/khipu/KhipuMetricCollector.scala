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

package org.yupana.khipu

import com.typesafe.scalalogging.StrictLogging
import org.yupana.metrics.{ Metric, MetricCollector, MetricImpl }

object KhipuMetricCollector extends MetricCollector with StrictLogging {

  private def createMetric(name: String): MetricImpl = new MetricImpl(name, this)

  object Metrics {
    val allocateBlock: MetricImpl = createMetric("allocateBlock")
    val findFreeBlock: MetricImpl = createMetric("findFreeBlocks")
    val put: MetricImpl = createMetric("put")
    val merge: MetricImpl = createMetric("merge")
    val splitAndWriteLeafBlocks: MetricImpl = createMetric("splitAndWriteLeafBlocks")
    val splitAndWriteNodeBlocks: MetricImpl = createMetric("splitAndWriteNodeBlocks")
    val writeLeafBlock: MetricImpl = createMetric("writeLeafBlock")
    val writeNodeBlock: MetricImpl = createMetric("writeNodeBlock")
    val load: MetricImpl = createMetric("load")
  }

  import Metrics._

  override def allMetrics: Seq[Metric] = Seq(
    allocateBlock,
    findFreeBlock,
    put,
    splitAndWriteNodeBlocks,
    splitAndWriteLeafBlocks,
    writeNodeBlock,
    writeLeafBlock,
    merge,
    load
  )

  override def id: String = "KhipuStorage"

  override def operationName: String = "none"

  override def partitionId: Option[String] = None

  override def dynamicMetric(name: String): Metric =
    throw new UnsupportedOperationException("Dynamic metrics are not supported")

  override def isEnabled: Boolean = true
  override def checkpoint(): Unit = {}

  override def metricUpdated(metric: Metric, time: Long): Unit = {}

  def reset(): Unit = {
    allMetrics.foreach(_.reset())
  }

  def logStat(showNulls: Boolean = true): Unit = {

    println("Khipu performance statistics")
    allMetrics.foreach { metric =>
      val time = metric.time
      val count = metric.count
      if (showNulls || time > 0 || count > 0)
        println(s" ==> stage: ${metric.name}; time: ${formatNanoTime(time)}; count: $count")
    }
  }

  private def formatNanoTime(value: Long): String = {
    new java.text.DecimalFormat("#.##########").format(value / 1000000000.0)
  }
}
