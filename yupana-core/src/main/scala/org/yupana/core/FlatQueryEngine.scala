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

import org.joda.time.Interval
import org.yupana.api.schema.Table
import org.yupana.core.dao.{ QueryMetricsFilter, RollupMetaDao, TsdbQueryMetricsDao }
import org.yupana.core.model.QueryStates.QueryState
import org.yupana.core.model.{ UpdateInterval, TsdbQueryMetrics }

class FlatQueryEngine(metricsDao: TsdbQueryMetricsDao, rollupMetaDao: RollupMetaDao) {
  def getUpdatesIntervals(
      tableName: String,
      rollupInterval: Interval
  ): Iterable[UpdateInterval] = {
    rollupMetaDao.getUpdatesIntervals(tableName, rollupInterval)
  }

  def deleteMetrics(filter: QueryMetricsFilter): Int = {
    metricsDao.deleteMetrics(filter)
  }

  def setQueryState(filter: QueryMetricsFilter, queryState: QueryState): Unit = {
    metricsDao.setQueryState(filter, queryState)
  }

  def queriesByFilter(filter: Option[QueryMetricsFilter], limit: Option[Int]): Iterable[TsdbQueryMetrics] = {
    metricsDao.queriesByFilter(filter, limit)
  }

  def getRollupSpecialField(fieldName: String, table: Table): Option[Long] = {
    rollupMetaDao.getRollupSpecialField(fieldName, table)
  }
}
