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

package org.yupana.core.providers

import org.joda.time.Interval
import org.yupana.api.Time
import org.yupana.api.query.{ Result, SimpleResult }
import org.yupana.api.types.DataType
import org.yupana.core.FlatQueryEngine
import org.yupana.core.sql.parser.TimestampPeriodValue

object UpdatesIntervalsProvider {
  import org.yupana.core.model.UpdateInterval._

  def handleGetUpdatesIntervals(
      flatQueryEngine: FlatQueryEngine,
      tableName: String,
      updatedAtPeriod: TimestampPeriodValue
  ): Result = {

    val updateInterval =
      new Interval(updatedAtPeriod.from.value.toDateTime.getMillis, updatedAtPeriod.to.value.toDateTime.getMillis)
    val updatesIntervals = flatQueryEngine.getUpdatesIntervals(tableName, updateInterval)
    val data: Iterator[Array[Any]] = updatesIntervals.map { period =>
      Array[Any](
        period.updatedAt.map(t => Time(t)).orNull,
        Time(period.from),
        Time(period.to),
      )
    }.iterator

    val queryFieldNames = List(
      updatedAtColumn,
      fromColumn,
      toColumn
    )

    val queryFieldTypes = List(
      DataType[Time],
      DataType[Time],
      DataType[Time],
    )

    SimpleResult("UPDATES_INTERVALS", queryFieldNames, queryFieldTypes, data)
  }
}
