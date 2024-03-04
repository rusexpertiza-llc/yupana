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
import org.yupana.metrics.MetricReporter

class StandaloneMetricCollector(
    query: Query,
    user: String,
    operationName: String,
    metricsUpdateInterval: Int,
    reporter: MetricReporter[MetricQueryCollector]
) extends StandardMetricCollector(
      query,
      user,
      operationName,
      metricsUpdateInterval,
      isSparkQuery = false,
      reporter = reporter
    ) {
  override val partitionId: Option[String] = None
}
