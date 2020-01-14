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

import com.typesafe.scalalogging.StrictLogging
import org.yupana.api.query.Query
import org.yupana.core.model.QueryStates.QueryState
import org.yupana.core.model.TsdbQueryMetrics._
import org.yupana.core.model.{ MetricData, QueryStates }
import org.yupana.core.utils.metric.PersistentMetricQueryCollector._

import scala.collection.{ Seq, mutable }

class PersistentMetricQueryCollector(collectorContext: QueryCollectorContext, query: Query)
    extends MetricQueryCollector
    with StrictLogging {

  override val isEnabled: Boolean = true

  private val operationName: String = collectorContext.operationName
  private val metricsUpdateInterval: Int = collectorContext.metricsUpdateInterval
  val uuid: String = query.uuid

  private def createMetric(qualifier: String): PersistentMetricImpl =
    PersistentMetricImpl(collectorContext, qualifier, query.uuid, this)

  override val createDimensionFilters: PersistentMetricImpl = createMetric(createDimensionFiltersQualifier)
  override val createScans: PersistentMetricImpl = createMetric(createScansQualifier)
  override val scan: PersistentMetricImpl = createMetric(scanQualifier)
  override val parseScanResult: PersistentMetricImpl = createMetric(parseScanResultQualifier)
  override val dimensionValuesForIds: PersistentMetricImpl = createMetric(dimensionValuesForIdsQualifier)
  override val readExternalLinks: PersistentMetricImpl = createMetric(readExternalLinksQualifier)
  override val extractDataComputation: PersistentMetricImpl = createMetric(extractDataComputationQualifier)
  override val filterRows: PersistentMetricImpl = createMetric(filterRowsQualifier)
  override val windowFunctions: PersistentMetricImpl = createMetric(windowFunctionsQualifier)
  override val reduceOperation: PersistentMetricImpl = createMetric(reduceOperationQualifier)
  override val postFilter: PersistentMetricImpl = createMetric(postFilterQualifier)
  override val collectResultRows: PersistentMetricImpl = createMetric(collectResultRowsQualifier)
  override val dictionaryScan: PersistentMetricImpl = createMetric(dictionaryScanQualifier)

  private val queryRowKey = collectorContext.metricsDao().initializeQueryMetrics(query, collectorContext.sparkQuery)
  logger.info(s"$queryRowKey - ${query.uuidLog}; operation: $operationName started, query: $query")

  private val dynamicMetrics = mutable.Map.empty[String, PersistentMetricImpl]
  private val startTime = System.nanoTime()
  private var lastSaveTime = startTime

  def getMetrics: Seq[PersistentMetricImpl] =
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
    )

  def getAndResetMetricsData: Map[String, MetricData] = {
    (dynamicMetrics.values ++ getMetrics).map { m =>
      val cnt = m.count.sumThenReset()
      val time = asSeconds(m.time.sumThenReset())
      val speed = if (time != 0) cnt.toDouble / time else 0.0
      val data = MetricData(cnt, time, speed)
      m.name -> data
    }.toMap
  }

  def saveMetricsIfItsTime(end: Long): Unit = {
    if (asSeconds(end - lastSaveTime) > metricsUpdateInterval) {
      saveQueryMetrics(QueryStates.Running)
      lastSaveTime = end
    }
  }

  def saveQueryMetrics(state: QueryState): Unit = {
    val duration = totalDuration
    collectorContext
      .metricsDao()
      .updateQueryMetrics(queryRowKey, state, duration, getAndResetMetricsData, collectorContext.sparkQuery)
  }

  private def totalDuration: Double = {
    val currentTime = System.nanoTime()
    if (currentTime > startTime) asSeconds(currentTime - startTime)
    else asSeconds(startTime - currentTime)
  }

  override def finish(): Unit = {

    getMetrics.sortBy(_.name).foreach { metric =>
      logger.info(
        s"$queryRowKey - ${query.uuidLog}; stage: ${metric.name}; time: ${asSeconds(metric.time.sum)}; count: ${metric.count.sum}"
      )
    }
    saveQueryMetrics(QueryStates.Finished)
    logger.info(
      s"$queryRowKey - ${query.uuidLog}; operation: $operationName finished; time: $totalDuration; query: $query"
    )
  }

  override def setRunningPartitions(partitions: Int): Unit = {
    collectorContext.metricsDao().setRunningPartitions(queryRowKey, partitions)
  }

  override def finishPartition(): Unit = {
    val restPartitions = collectorContext.metricsDao().decrementRunningPartitions(queryRowKey)
    saveQueryMetrics(QueryStates.Running)

    if (restPartitions <= 0) {
      finish()
    }
  }

  override def dynamicMetric(name: String): Metric = dynamicMetrics.getOrElseUpdate(name, createMetric(name))
}

object PersistentMetricQueryCollector {
  def asSeconds(n: Long): Double = n / 1000000000.0
}

case class PersistentMetricImpl(
    collectorContext: QueryCollectorContext,
    name: String,
    queryId: String,
    metricCollector: PersistentMetricQueryCollector,
    count: LongAdder = new LongAdder(),
    time: LongAdder = new LongAdder()
) extends Metric {

  override def measure[T](cnt: Int)(f: => T): T = {
    if (!collectorContext.queryActive) {
      throw new IllegalStateException(s"Metric $name: query $queryId was cancelled!")
    }
    try {
      val start = System.nanoTime()
      val result = f
      count.add(cnt)
      val end = System.nanoTime()
      time.add(end - start)
      metricCollector.saveMetricsIfItsTime(end)
      result
    } catch {
      case e: Throwable =>
        collectorContext.timer.cancel()
        throw e
    }
  }
}
