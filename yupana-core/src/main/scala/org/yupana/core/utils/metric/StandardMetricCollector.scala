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
import org.yupana.core.model.QueryStates

import scala.collection.mutable

abstract class StandardMetricCollector(
    override val query: Query,
    override val operationName: String,
    metricsUpdateInterval: Int,
    val isSparkQuery: Boolean,
    reporter: MetricReporter[MetricQueryCollector]
) extends MetricQueryCollector {

  import org.yupana.core.model.TsdbQueryMetrics._

  private var lastSaveTime: Long = -1L

  private val dynamicMetrics = mutable.Map.empty[String, MetricImpl]

  override def start(): Unit = {
    super.start()
    reporter.start(this, partitionId)
  }

  private def createMetric(qualifier: String): MetricImpl = new MetricImpl(qualifier, this)

  override val createDimensionFilters: MetricImpl = createMetric(createDimensionFiltersQualifier)
  override val createScans: MetricImpl = createMetric(createScansQualifier)
  override val scan: MetricImpl = createMetric(scanQualifier)
  override val readExternalLinks: MetricImpl = createMetric(readExternalLinksQualifier)
  override val extractDataComputation: MetricImpl = createMetric(extractDataComputationQualifier)
  override val filterRows: MetricImpl = createMetric(filterRowsQualifier)
  override val windowFunctions: MetricImpl = createMetric(windowFunctionsQualifier)
  override val reduceOperation: MetricImpl = createMetric(reduceOperationQualifier)
  override val postFilter: MetricImpl = createMetric(postFilterQualifier)
  override val collectResultRows: MetricImpl = createMetric(collectResultRowsQualifier)

  override def dynamicMetric(name: String): Metric = dynamicMetrics.getOrElseUpdate(name, createMetric(name))

  override val isEnabled: Boolean = true

  override def finish(): Unit = {
    super.finish()
    reporter.saveQueryMetrics(this, partitionId, QueryStates.Finished)
    reporter.finish(this, partitionId)
  }

  override def metricUpdated(metric: Metric, time: Long): Unit = {
    if (MetricCollector.asSeconds(time - lastSaveTime) > metricsUpdateInterval) {
      reporter.saveQueryMetrics(this, partitionId, QueryStates.Running)
      lastSaveTime = time
    }
  }

  override def checkpoint(): Unit = reporter.saveQueryMetrics(this, partitionId, QueryStates.Running)

  def allMetrics: Seq[MetricImpl] =
    Seq(
      createDimensionFilters,
      createScans,
      scan,
      readExternalLinks,
      extractDataComputation,
      filterRows,
      windowFunctions,
      reduceOperation,
      postFilter,
      collectResultRows
    ) ++ dynamicMetrics.values
}
