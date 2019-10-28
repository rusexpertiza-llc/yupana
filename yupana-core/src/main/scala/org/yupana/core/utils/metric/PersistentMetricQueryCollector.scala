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

import java.util.TimerTask
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
    PersistentMetricImpl(collectorContext, qualifier, query.uuid)

  override val createQueries: PersistentMetricImpl = createMetric(createQueriesQualifier)
  override val createDimensionFilters: PersistentMetricImpl = createMetric(createDimensionFiltersQualifier)
  override val createScans: PersistentMetricImpl = createMetric(createScansQualifier)
  override val loadTags: PersistentMetricImpl = createMetric(loadTagsQualifier)
  override val filterRows: PersistentMetricImpl = createMetric(filterRowsQualifier)
  override val windowFunctions: PersistentMetricImpl = createMetric(windowFunctionsQualifier)
  override val reduceOperation: PersistentMetricImpl = createMetric(reduceOperationQualifier)
  override val postFilter: PersistentMetricImpl = createMetric(postFilterQualifier)
  override val collectResultRows: PersistentMetricImpl = createMetric(collectResultRowsQualifier)
  override val extractDataTags: PersistentMetricImpl = createMetric(extractDataTagsQualifier)
  override val extractDataComputation: PersistentMetricImpl = createMetric(extractDataComputationQualifier)
  override val scan: PersistentMetricImpl = createMetric(getResultQualifier)
  override val parseScanResult: PersistentMetricImpl = createMetric(parseResultQualifier)

  private val queryRowKey = collectorContext.metricsDao().initializeQueryMetrics(query, collectorContext.sparkQuery)
  logger.info(s"$queryRowKey - ${query.uuidLog}; operation: $operationName started, query: $query")

  private val dynamicMetrics = mutable.Map.empty[String, PersistentMetricImpl]
  private val startTime = System.nanoTime()

  if (!collectorContext.sparkQuery) {
    collectorContext.timer.schedule(
      new TimerTask {
        override def run(): Unit = {
          try {
            updateQueryMetrics(QueryStates.Running)
          } catch {
            case _: Throwable =>
              collectorContext.queryActive = false
              collectorContext.timer.cancel()
          }
        }
      },
      0,
      metricsUpdateInterval
    )
  }

  def getMetrics: Seq[PersistentMetricImpl] =
    Seq(
      createQueries,
      createDimensionFilters,
      createScans,
      loadTags,
      filterRows,
      windowFunctions,
      reduceOperation,
      postFilter,
      collectResultRows,
      extractDataTags,
      extractDataComputation,
      scan,
      parseScanResult
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

  private def totalDuration: Double = asSeconds(System.nanoTime() - startTime)

  private def updateQueryMetrics(state: QueryState): Unit = {
    val duration = totalDuration
    collectorContext
      .metricsDao()
      .updateQueryMetrics(queryRowKey, state, duration, getAndResetMetricsData, collectorContext.sparkQuery)
  }

  override def finish(): Unit = {
    if (!collectorContext.sparkQuery) {
      collectorContext.timer.cancel()
    }
    getMetrics.sortBy(_.name).foreach { metric =>
      logger.info(
        s"$queryRowKey - ${query.uuidLog}; stage: ${metric.name}; time: ${asSeconds(metric.time.sum)}; count: ${metric.count.sum}"
      )
    }
    updateQueryMetrics(QueryStates.Finished)
    logger.info(
      s"$queryRowKey - ${query.uuidLog}; operation: $operationName finished; time: $totalDuration; query: $query"
    )
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
      time.add(System.nanoTime() - start)
      result
    } catch {
      case e: Throwable =>
        collectorContext.timer.cancel()
        throw e
    }
  }
}
