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

package org.yupana.spark

import org.yupana.core.dao.TsdbQueryMetricsDao
import org.yupana.core.utils.metric.{ InternalMetricData, MetricQueryCollector }
import org.yupana.metrics.{ MetricReporter, QueryStates }

import scala.collection.mutable.ListBuffer

class SparkMetricsReporter(metricsDao: () => TsdbQueryMetricsDao)
    extends MetricReporter[MetricQueryCollector]
    with Serializable {

  private val buffer: ListBuffer[InternalMetricData] = ListBuffer.empty
  override def start(mc: MetricQueryCollector, partitionId: Option[String]): Unit = {
    metricsDao().saveQueryMetrics(List(InternalMetricData.fromMetricCollector(mc, partitionId, QueryStates.Running)))
  }

  override def finish(mc: MetricQueryCollector, partitionId: Option[String]): Unit = {
    flush()
  }

  override def saveQueryMetrics(
      mc: MetricQueryCollector,
      partitionId: Option[String],
      state: QueryStates.QueryState
  ): Unit = {
    buffer += InternalMetricData.fromMetricCollector(mc, partitionId, state)
  }

  private def flush(): Unit = {
    metricsDao().saveQueryMetrics(buffer.toList)
    buffer.clear()
  }
}
