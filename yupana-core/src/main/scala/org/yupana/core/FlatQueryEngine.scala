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

package org.yupana.core

import org.yupana.core.dao.{ QueryMetricsFilter, ChangelogDao, TsdbQueryMetricsDao }
import org.yupana.core.model.{ TsdbQueryMetrics, UpdateInterval }
import org.yupana.core.providers.UpdatesIntervalsProvider.UpdatesIntervalsFilter
import org.yupana.metrics.QueryStates.QueryState

class FlatQueryEngine(metricsDao: TsdbQueryMetricsDao, changelogDao: ChangelogDao) {
  def getUpdatesIntervals(filter: UpdatesIntervalsFilter = UpdatesIntervalsFilter.empty): Iterable[UpdateInterval] = {
    changelogDao.getUpdatesIntervals(
      filter.tableName,
      filter.updatedAfter.map(_.toInstant.toEpochMilli),
      filter.updatedBefore.map(_.toInstant.toEpochMilli),
      filter.recalculatedAfter.map(_.toInstant.toEpochMilli),
      filter.recalculatedBefore.map(_.toInstant.toEpochMilli),
      filter.updatedBy
    )
  }

  def deleteMetrics(filter: QueryMetricsFilter): Int = {
    metricsDao.deleteMetrics(filter)
  }

  def setQueryState(filter: QueryMetricsFilter, queryState: QueryState): Unit = {
//    metricsDao.setQueryState(filter, queryState)
  }

  def queriesByFilter(filter: Option[QueryMetricsFilter], limit: Option[Int]): Iterator[TsdbQueryMetrics] = {
    metricsDao.queriesByFilter(filter, limit)
  }
}
