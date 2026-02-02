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

package org.yupana.core.dao

import org.yupana.api.query.Expression.Condition
import org.yupana.core.{ MapReducible, QueryContext }
import org.yupana.core.model.{ BatchDataset, InternalQuery, DatasetSchema }
import org.yupana.core.utils.metric.MetricQueryCollector

trait TSReadingDao[Collection[_], IdType] {

  val extractBatchSize: Int

  def query(
      query: InternalQuery,
      queryContext: QueryContext,
      datasetSchema: DatasetSchema,
      metricCollector: MetricQueryCollector
  ): Collection[BatchDataset]

  def mapReduceEngine(metricQueryCollector: MetricQueryCollector): MapReducible[Collection]

  def isSupportedCondition(condition: Condition): Boolean
}
