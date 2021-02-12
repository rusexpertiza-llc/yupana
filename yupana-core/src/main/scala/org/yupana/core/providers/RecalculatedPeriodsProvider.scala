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

import org.joda.time.LocalDateTime
import org.yupana.api.Time
import org.yupana.api.query.{ Result, SimpleResult }
import org.yupana.api.types.DataType
import org.yupana.core.FlatQueryEngine

object RecalculatedPeriodsProvider {
  import org.yupana.core.model.RecalculatedPeriod._

  def handleGetRecalculatedPeriods(
      flatQueryEngine: FlatQueryEngine,
      rollupDateFrom: LocalDateTime,
      rollupDateTo: LocalDateTime
  ): Result = {

    val recalculatedPeriods = flatQueryEngine.getRecalculatedPeriods(rollupDateFrom, rollupDateTo)
    val data: Iterator[Array[Any]] = recalculatedPeriods.map { period =>
      Array[Any](
        Time(period.rollupTime),
        Time(period.from),
        Time(period.to)
      )
    }.iterator

    val queryFieldNames = List(
      rollupTimeColumn,
      fromColumn,
      toColumn
    )

    val queryFieldTypes = List(
      DataType[Time],
      DataType[Time],
      DataType[Time]
    )

    SimpleResult("RECALCULATED_INTERVALS", queryFieldNames, queryFieldTypes, data)
  }

  def handleGetInvalidatedBaseTimes(flatQueryEngine: FlatQueryEngine): Result = {
    val invalidatedBaseTimes = flatQueryEngine.getInvalidatedBaseTimes
    val data: Iterator[Array[Any]] = invalidatedBaseTimes.map { baseTime =>
      Array[Any](
        Time(baseTime)
      )
    }.iterator

    val queryFieldNames = List(
      fromColumn
    )

    val queryFieldTypes = List(
      DataType[Time]
    )

    SimpleResult("INVALIDATED_BASE_TIMES", queryFieldNames, queryFieldTypes, data)
  }
}
