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

import java.util.concurrent.atomic.LongAdder

import org.yupana.api.query.Query
import org.yupana.core.model.QueryStates

import scala.collection.mutable

class StandardMetricCollector(
    val query: Query,
    val operationName: String,
    metricsUpdateInterval: Int,
    val isSparkQuery: Boolean,
    reporter: MetricReporter
) extends MetricQueryCollector {

  import org.yupana.core.model.TsdbQueryMetrics._

  override val startTime: Long = System.nanoTime()
  private var endTime: Long = startTime
  private var lastSaveTime: Long = -1L

  override def resultTime: Long = endTime - startTime

  private val dynamicMetrics = mutable.Map.empty[String, MetricImpl]

  reporter.start(this)

  private def createMetric(qualifier: String): MetricImpl = new MetricImpl(qualifier, this)

  override val createDimensionFilters: MetricImpl = createMetric(createDimensionFiltersQualifier)
  override val createScans: MetricImpl = createMetric(createScansQualifier)
  override val scan: MetricImpl = createMetric(scanQualifier)
  override val parseScanResult: MetricImpl = createMetric(parseScanResultQualifier)
  override val dimensionValuesForIds: MetricImpl = createMetric(dimensionValuesForIdsQualifier)
  override val readExternalLinks: MetricImpl = createMetric(readExternalLinksQualifier)
  override val extractDataComputation: MetricImpl = createMetric(extractDataComputationQualifier)
  override val filterRows: MetricImpl = createMetric(filterRowsQualifier)
  override val windowFunctions: MetricImpl = createMetric(windowFunctionsQualifier)
  override val reduceOperation: MetricImpl = createMetric(reduceOperationQualifier)
  override val postFilter: MetricImpl = createMetric(postFilterQualifier)
  override val collectResultRows: MetricImpl = createMetric(collectResultRowsQualifier)
  override val dictionaryScan: MetricImpl = createMetric(dictionaryScanQualifier)

  override def dynamicMetric(name: String): Metric = dynamicMetrics.getOrElseUpdate(name, createMetric(name))

  override val isEnabled: Boolean = true

  override def finish(): Unit = {
    endTime = System.nanoTime()
    reporter.saveQueryMetrics(this, QueryStates.Finished)
    reporter.finish(this)
  }

  override def metricUpdated(metric: Metric, time: Long): Unit = {
    if (StandardMetricCollector.asSeconds(time - lastSaveTime) > metricsUpdateInterval) {
      reporter.saveQueryMetrics(this, QueryStates.Running)
      lastSaveTime = time
    }
  }

  override def checkpoint(): Unit = reporter.saveQueryMetrics(this, QueryStates.Running)

  override def setRunningPartitions(partitions: Int): Unit = reporter.setRunningPartitions(this, partitions)

  override def finishPartition(): Unit = reporter.finishPartition(this)

  def allMetrics: Seq[MetricImpl] =
    Seq(
      createDimensionFilters,
      createScans,
      scan,
      parseScanResult,
      dimensionValuesForIds,
      readExternalLinks,
      extractDataComputation,
      filterRows,
      windowFunctions,
      reduceOperation,
      postFilter,
      collectResultRows,
      dictionaryScan
    ) ++ dynamicMetrics.values
}

class MetricImpl(
    val name: String,
    metricCollector: MetricQueryCollector
) extends Metric {

  private val countAdder: LongAdder = new LongAdder()
  private val timeAdder: LongAdder = new LongAdder()

  override def time: Long = timeAdder.sum()
  override def count: Long = countAdder.sum()

  override def measure[T](cnt: Int)(f: => T): T = {
    val start = System.nanoTime()
    val result = f
    countAdder.add(cnt)
    val end = System.nanoTime()
    timeAdder.add(end - start)
    metricCollector.metricUpdated(this, end)
    result
  }
}

object StandardMetricCollector {
  def asSeconds(n: Long): Double = n / 1000000000.0
}
