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

package org.yupana.akka

import org.joda.time.LocalDateTime
import org.yupana.api.Time
import org.yupana.api.query.{ Result, SimpleResult }
import org.yupana.api.types.DataType
import org.yupana.core.model.InvalidPeriod
import org.yupana.core.{ GetInvalidPeriodsQuery, TSDB }

object InvalidPeriodsProvider {

  def handleGetInvalidPeriods(tsdb: TSDB, rollupDateFrom: LocalDateTime, rollupDateTo: LocalDateTime): Result = {
    import org.yupana.core.model.InvalidPeriod._

    val query = GetInvalidPeriodsQuery(rollupDateFrom, rollupDateTo)
    val invalidPeriods = tsdb.queryEngine.execute[Iterable[InvalidPeriod]](query)
    val data: Iterator[Array[Any]] = invalidPeriods.map { period =>
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

    SimpleResult("INVALID_INTERVALS", queryFieldNames, queryFieldTypes, data)
  }
}
