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

import org.apache.spark.SparkEnv
import org.yupana.api.query.Query
import org.yupana.core.utils.metric.{ MetricQueryCollector, StandardMetricCollector }
import org.yupana.metrics.MetricReporter

class SparkMetricCollector(
    query: Query,
    user: String,
    opName: String,
    metricsUpdateInterval: Int,
    reporter: MetricReporter[MetricQueryCollector]
) extends StandardMetricCollector(
      query,
      user,
      opName,
      metricsUpdateInterval,
      isSparkQuery = true,
      reporter = reporter
    ) {
  override def partitionId: Option[String] = Some(SparkEnv.get.executorId)
}
