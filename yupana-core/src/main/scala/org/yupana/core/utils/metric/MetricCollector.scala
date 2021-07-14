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

class MetricCollector(val query: Query, collectorContext: QueryCollectorContext, reporter: MetricReporter)
    extends MetricQueryCollector {

  import org.yupana.core.model.TsdbQueryMetrics._

  override val startTime: Long = System.nanoTime()
  private var endTime: Long = startTime
  private var lastSaveTime: Long = -1L

  override def resultTime: Long = endTime - startTime

  private val dynamicMetrics = mutable.Map.empty[String, MetricImpl]
  private val metricsUpdateInterval: Int = collectorContext.metricsUpdateInterval

  reporter.start(this)

  private def createMetric(qualifier: String): MetricImpl = new MetricImpl(qualifier, this)

  val createDimensionFilters: MetricImpl = createMetric(createDimensionFiltersQualifier)
  val createScans: MetricImpl = createMetric(createScansQualifier)
  val scan: MetricImpl = createMetric(scanQualifier)
  val parseScanResult: MetricImpl = createMetric(parseScanResultQualifier)
  val dimensionValuesForIds: MetricImpl = createMetric(dimensionValuesForIdsQualifier)
  val readExternalLinks: MetricImpl = createMetric(readExternalLinksQualifier)
  val extractDataComputation: MetricImpl = createMetric(extractDataComputationQualifier)
  val filterRows: MetricImpl = createMetric(filterRowsQualifier)
  val windowFunctions: MetricImpl = createMetric(windowFunctionsQualifier)
  val reduceOperation: MetricImpl = createMetric(reduceOperationQualifier)
  val postFilter: MetricImpl = createMetric(postFilterQualifier)
  val collectResultRows: MetricImpl = createMetric(collectResultRowsQualifier)
  val dictionaryScan: MetricImpl = createMetric(dictionaryScanQualifier)

  override def operationName: String = collectorContext.operationName

  override def dynamicMetric(name: String): Metric = dynamicMetrics.getOrElseUpdate(name, createMetric(name))

  override def isEnabled: Boolean = true

  override def finish(): Unit = {
    endTime = System.nanoTime()
    reporter.saveQueryMetrics(this, QueryStates.Finished)
    reporter.finish(this)
  }

  override def metricUpdated(metric: Metric, time: Long): Unit = {
    if (MetricCollector.asSeconds(time - lastSaveTime) > metricsUpdateInterval) {
      reporter.saveQueryMetrics(this, QueryStates.Running)
      lastSaveTime = time
    }

  }

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

object MetricCollector {
  def asSeconds(n: Long): Double = n / 1000000000.0
}
