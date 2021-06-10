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

import com.typesafe.scalalogging.StrictLogging
import org.yupana.api.Time
import org.yupana.api.query.{ Result, SimpleResult }
import org.yupana.api.types.DataType
import org.yupana.core.{ FlatQueryEngine, UpdatesIntervalsFilter }
import org.yupana.core.sql.parser._

object UpdatesIntervalsProvider extends StrictLogging {
  import org.yupana.core.model.UpdateInterval._

  def handleGetUpdatesIntervals(flatQueryEngine: FlatQueryEngine, maybeCondition: Option[Condition]): Result = {

    def addSimpleCondition(f: UpdatesIntervalsFilter, c: Condition): UpdatesIntervalsFilter = {
      c match {
        case Eq(FieldName("table"), Constant(StringValue(value))) => f.withTableName(value)
        case Eq(Constant(StringValue(value)), FieldName("table")) => f.withTableName(value)
        case BetweenCondition(FieldName("updated_at"), TimestampValue(from), TimestampValue(to)) =>
          f.withFrom(from).withTo(to)
        case Eq(FieldName("updated_by"), Constant(StringValue(value))) => f.withBy(value)
        case Eq(Constant(StringValue(value)), FieldName("updated_by")) => f.withBy(value)
        case c =>
          logger.warn(s"Unsapported condition: $c")
          f
      }
    }

    val filter = maybeCondition match {
      case None          => UpdatesIntervalsFilter.empty
      case Some(And(cs)) => cs.foldLeft(UpdatesIntervalsFilter.empty)((f, c) => addSimpleCondition(f, c))
      case Some(c)       => addSimpleCondition(UpdatesIntervalsFilter.empty, c)
    }

    val updatesIntervals = flatQueryEngine.getUpdatesIntervals(filter)
    val data: Iterator[Array[Any]] = updatesIntervals.map { period =>
      Array[Any](
        period.table,
        Time(period.updatedAt),
        Time(period.from),
        Time(period.to),
        period.updatedBy
      )
    }.iterator

    val queryFieldNames = List(
      tableColumn,
      updatedAtColumn,
      fromColumn,
      toColumn,
      updatedByColumn
    )

    val queryFieldTypes = List(
      DataType[String],
      DataType[Time],
      DataType[Time],
      DataType[Time],
      DataType[String]
    )

    SimpleResult("UPDATES_INTERVALS", queryFieldNames, queryFieldTypes, data)
  }
}
