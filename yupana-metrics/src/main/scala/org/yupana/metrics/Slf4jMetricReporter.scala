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

package org.yupana.metrics

import com.typesafe.scalalogging.StrictLogging

class Slf4jMetricReporter[C <: MetricCollector] extends MetricReporter[C] with StrictLogging with Serializable {
  override def start(mc: C, partitionId: Option[String]): Unit = {
    logger.info(s"${mc.fullId}; operation: ${mc.operationName} started, meta: ${mc.meta}")
  }

  override def finish(mc: C, partitionId: Option[String]): Unit = {
    import Slf4jMetricReporter._
    mc.allMetrics.sortBy(_.name).foreach { metric =>
      logger.info(
        s"${mc.fullId}; stage: ${metric.name}; time: ${formatNanoTime(metric.time)}; count: ${metric.count}"
      )
    }
    logger.info(
      s"${mc.fullId}; operation: ${mc.operationName} finished; time: ${formatNanoTime(mc.resultDuration)}; meta: ${mc.meta}"
    )
  }

  override def saveQueryMetrics(mc: C, partitionId: Option[String], state: QueryStates.QueryState): Unit = {}
}

object Slf4jMetricReporter {
   def formatNanoTime(value: Long): String = {
    new java.text.DecimalFormat("#.##########").format(value / 1000000000.0)
  }
}
