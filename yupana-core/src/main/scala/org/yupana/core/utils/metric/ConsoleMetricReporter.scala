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

import com.typesafe.scalalogging.StrictLogging
import org.yupana.core.model.QueryStates

class ConsoleMetricReporter[C <: MetricCollector] extends MetricReporter[C] with StrictLogging {

  override def start(mc: C): Unit = {
    logger.info(s"${mc.id}; operation: ${mc.operationName} started, meta: ${mc.meta}")
  }

  override def finish(mc: C): Unit = {
    import ConsoleMetricReporter._

    mc.allMetrics.sortBy(_.name).foreach { metric =>
      logger.info(
        s"${mc.id}; stage: ${metric.name}; time: ${formatNanoTime(metric.time)}; count: ${metric.count}"
      )
    }
    logger.info(
      s"${mc.id}; operation: ${mc.operationName} finished; time: ${formatNanoTime(mc.resultTime)}; meta: ${mc.meta}"
    )
  }

  override def saveQueryMetrics(mc: C, state: QueryStates.QueryState): Unit = {}
  override def setRunningPartitions(mc: C, partitions: Int): Unit = {}
  override def finishPartition(mc: C): Unit = {}
}

object ConsoleMetricReporter {
  private def formatNanoTime(value: Long): String = {
    new java.text.DecimalFormat("#.##########").format(value / 1000000000.0)
  }
}
