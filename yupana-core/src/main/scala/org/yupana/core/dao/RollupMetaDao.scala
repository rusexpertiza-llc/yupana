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

import org.joda.time.Interval
import org.yupana.api.query.DataPoint
import org.yupana.api.schema.Table
import org.yupana.core.model.UpdateInterval

trait RollupMetaDao {
  def putUpdatesIntervals(tableName: String, periods: Seq[UpdateInterval]): Unit
  def getUpdatesIntervals(
      tableName: String,
      updatedAfter: Option[Long],
      updatedBefore: Option[Long]
  ): Iterable[UpdateInterval]
  def getUpdatesIntervals(tableName: String, interval: Interval): Iterable[UpdateInterval] = {
    getUpdatesIntervals(tableName, Some(interval.getStartMillis), Some(interval.getEndMillis))
  }

  def getRollupSpecialField(fieldName: String, table: Table): Option[Long]
  def putRollupSpecialField(fieldName: String, value: Long, table: Table): Unit
}

object RollupMetaDao {

  object SpecialField {
    val PREV_ROLLUP_TIME: String = "prev_run_ts"
  }

  def dataPointsToUpdatedIntervals(dps: Seq[DataPoint], timeGranularity: Long): Seq[UpdateInterval] = {
    val nowMillis = System.currentTimeMillis()
    dps
      .map(dp => dp.time - dp.time % timeGranularity)
      .distinct
      .map { baseTime =>
        UpdateInterval(from = baseTime, to = baseTime + timeGranularity, Some(nowMillis))
      }
  }
}
