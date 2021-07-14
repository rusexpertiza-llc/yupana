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

class ConsoleMetricQueryCollector extends MetricReporter with StrictLogging {

  override def start(mc: MetricQueryCollector): Unit = {
    logger.info(s"${mc.query.id} - ${mc.query.uuidLog}; operation: ${mc.operationName} started, query: ${mc.query}")
  }

  override def finish(mc: MetricQueryCollector): Unit = {
    import ConsoleMetricQueryCollector._

    mc.allMetrics.sortBy(_.name).foreach { metric =>
      logger.info(
        s"${mc.query.uuidLog}; stage: ${metric.name}; time: ${formatNanoTime(metric.time)}; count: ${metric.count}"
      )
    }
    logger.info(
      s"${mc.query.uuidLog}; operation: ${mc.operationName} finished; time: ${formatNanoTime(mc.resultTime)}; query: ${mc.query}"
    )
  }

  override def saveQueryMetrics(mc: MetricQueryCollector, state: QueryStates.QueryState): Unit = {}
  override def setRunningPartitions(mc: MetricQueryCollector, partitions: Int): Unit = {}
  override def finishPartition(mc: MetricQueryCollector): Unit = {}
}

object ConsoleMetricQueryCollector {
  private def formatNanoTime(value: Long): String = {
    new java.text.DecimalFormat("#.##########").format(value / 1000000000.0)
  }
}
