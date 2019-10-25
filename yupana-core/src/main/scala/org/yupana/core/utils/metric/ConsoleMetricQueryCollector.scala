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

import java.util.concurrent.atomic.{ AtomicLong, LongAdder }

import com.typesafe.scalalogging.StrictLogging
import org.yupana.api.query.Query

import scala.collection.{ Seq, mutable }

class ConsoleMetricQueryCollector(query: Query, operationName: String) extends MetricQueryCollector with StrictLogging {

  override val isEnabled: Boolean = true

  val uuid: String = query.uuid

  override val createQueries = MetricImpl("createQueries")
  override val createDimensionFilters = MetricImpl("createQueries.tags")
  override val createScans = MetricImpl("createScans")
  override val loadTags = MetricImpl("loadTags")
  override val filterRows = MetricImpl("filterRows")
  override val windowFunctions = MetricImpl("windowFunctions")
  override val reduceOperation = MetricImpl("reduceOperation")
  override val postFilter = MetricImpl("postFilter")
  override val collectResultRows = MetricImpl("collectResultRows")
  override val extractDataTags = MetricImpl("extractData.tags")
  override val extractDataComputation = MetricImpl("extractData.computation")
  override val getResult = MetricImpl("getResult")
  override val parseResult = MetricImpl("parseResult")

  private val dynamicMetrics = mutable.Map.empty[String, MetricImpl]
  private val startTime = System.nanoTime()
  logger.info(s"${query.uuidLog}; operation: $operationName started, query: $query")

  def getMetrics: Seq[MetricImpl] =
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
      getResult,
      parseResult
    )

  override def finish(): Unit = {
    import ConsoleMetricQueryCollector._

    val resultTime = System.nanoTime() - startTime
    val metrics = (dynamicMetrics.values ++ getMetrics).toSeq
    metrics.sortBy(_.name).foreach { metric =>
      logger.info(
        s"${query.uuidLog}; stage: ${metric.name}; time: ${formatNanoTime(metric.time.sum())}; count: ${metric.count}"
      )
    }
    logger.info(
      s"${query.uuidLog}; operation: $operationName finished; time: ${formatNanoTime(resultTime)}; query: $query"
    )
  }

  override def dynamicMetric(name: String): Metric = dynamicMetrics.getOrElseUpdate(name, MetricImpl(name))
}

object ConsoleMetricQueryCollector {
  private def formatNanoTime(value: Long): String = {
    new java.text.DecimalFormat("#.##########").format(value / 1000000000.0)
  }
}

case class MetricImpl(name: String, count: AtomicLong = new AtomicLong(), time: LongAdder = new LongAdder())
    extends Metric {

  override def measure[T](f: => T): T = {
    val start = System.nanoTime()
    val result = f
    count.incrementAndGet()
    time.add(System.nanoTime() - start)
    result
  }
}
