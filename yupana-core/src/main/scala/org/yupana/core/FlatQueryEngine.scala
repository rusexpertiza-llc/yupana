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

import org.joda.time.DateTime
import org.yupana.core.dao.{ QueryMetricsFilter, RollupMetaDao, TsdbQueryMetricsDao }
import org.yupana.core.model.QueryStates.QueryState
import org.yupana.core.model.{ TsdbQueryMetrics, UpdateInterval }

case class UpdatesIntervalsFilter(
    maybeTableName: Option[String] = None,
    maybeFrom: Option[DateTime] = None,
    maybeTo: Option[DateTime] = None,
    maybeBy: Option[String] = None
) {
  def withTableName(tn: String): UpdatesIntervalsFilter = this.copy(maybeTableName = Some(tn))
  def withFrom(f: DateTime): UpdatesIntervalsFilter = this.copy(maybeFrom = Some(f))
  def withTo(t: DateTime): UpdatesIntervalsFilter = this.copy(maybeTo = Some(t))
  def withBy(ub: String): UpdatesIntervalsFilter = this.copy(maybeBy = Some(ub))
}

object UpdatesIntervalsFilter {
  val empty: UpdatesIntervalsFilter = UpdatesIntervalsFilter()
}

class FlatQueryEngine(metricsDao: TsdbQueryMetricsDao, rollupMetaDao: RollupMetaDao) {
  def getUpdatesIntervals(filter: UpdatesIntervalsFilter = UpdatesIntervalsFilter.empty): Iterable[UpdateInterval] = {
    rollupMetaDao.getUpdatesIntervals(
      filter.maybeTableName,
      filter.maybeFrom.map(_.getMillis),
      filter.maybeTo.map(_.getMillis),
      filter.maybeBy
    )
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
}
