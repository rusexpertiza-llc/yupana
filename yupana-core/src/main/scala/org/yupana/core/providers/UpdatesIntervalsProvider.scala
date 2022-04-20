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
import org.yupana.core.FlatQueryEngine
import org.yupana.core.sql.parser._

import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit

object UpdatesIntervalsProvider extends StrictLogging {
  import org.yupana.core.model.UpdateInterval._

  implicit class CaseInsensitiveRegex(sc: StringContext) {
    def ci = ("(?i)" + sc.parts.mkString).r
  }

  def handleGetUpdatesIntervals(flatQueryEngine: FlatQueryEngine, maybeCondition: Option[Condition]): Result = {
    val filter = createFilter(maybeCondition)
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

  def handleLastRecalculatedDay(flatQueryEngine: FlatQueryEngine, maybeCondition: Option[Condition]): Result = {
    val filter = createFilter(maybeCondition)
    val limit = filter.recalculatedBefore.getOrElse(OffsetDateTime.now().truncatedTo(ChronoUnit.DAYS))
    val it = flatQueryEngine.getUpdatesIntervals(filter.copy(recalculatedBefore = None)).iterator
    if (it.nonEmpty) {
      var maxDay = OffsetDateTime.MIN
      while (it.nonEmpty && !maxDay.isEqual(limit)) {
        val interval = it.next()
        if (interval.to.isAfter(maxDay)) {
          if (interval.to.isAfter(limit)) {
            maxDay = limit
          } else {
            maxDay = interval.to
          }
        }
      }
      SimpleResult("UPDATES_INTERVALS", List("day"), List(DataType[Time]), Iterator(Array(Time(maxDay))))
    } else
      SimpleResult("UPDATES_INTERVALS", List("day"), List(DataType[Time]), Iterator(Array(null)))
  }

  case class UpdatesIntervalsFilter(
      tableName: Option[String] = None,
      updatedAfter: Option[OffsetDateTime] = None,
      updatedBefore: Option[OffsetDateTime] = None,
      recalculatedAfter: Option[OffsetDateTime] = None,
      recalculatedBefore: Option[OffsetDateTime] = None,
      updatedBy: Option[String] = None
  ) {
    def withTableName(tn: String): UpdatesIntervalsFilter = this.copy(tableName = Some(tn))
    def withUpdatedAfter(f: OffsetDateTime): UpdatesIntervalsFilter = this.copy(updatedAfter = Some(f))
    def withUpdatedBefore(t: OffsetDateTime): UpdatesIntervalsFilter = this.copy(updatedBefore = Some(t))
    def withRecalculatedAfter(f: OffsetDateTime): UpdatesIntervalsFilter = this.copy(recalculatedAfter = Some(f))
    def withRecalculatedBefore(t: OffsetDateTime): UpdatesIntervalsFilter = this.copy(recalculatedBefore = Some(t))
    def withBy(ub: String): UpdatesIntervalsFilter = this.copy(updatedBy = Some(ub))
  }

  object UpdatesIntervalsFilter {
    val empty: UpdatesIntervalsFilter = UpdatesIntervalsFilter()
  }

  def createFilter(maybeCondition: Option[Condition]): UpdatesIntervalsFilter = {
    def addSimpleCondition(f: UpdatesIntervalsFilter, c: Condition): UpdatesIntervalsFilter = {
      c match {
        case Eq(FieldName(ci"table"), Constant(StringValue(value))) => f.withTableName(value)
        case Eq(Constant(StringValue(value)), FieldName(ci"table")) => f.withTableName(value)
        case BetweenCondition(FieldName(ci"updated_at"), TimestampValue(from), TimestampValue(to)) =>
          f.withUpdatedAfter(from).withUpdatedBefore(to)
        case BetweenCondition(FieldName(ci"recalculated_at"), TimestampValue(from), TimestampValue(to)) =>
          f.withRecalculatedAfter(from).withRecalculatedBefore(to)
        case BetweenCondition(FieldName(ci"recalculated_at"), TimestampValue(from), TimestampValue(to)) =>
          f.withRecalculatedAfter(from).withRecalculatedBefore(to)
        case Eq(FieldName(ci"updated_by"), Constant(StringValue(value))) => f.withBy(value)
        case Eq(Constant(StringValue(value)), FieldName(ci"updated_by")) => f.withBy(value)
        case c =>
          logger.warn(s"Unsupported condition: $c")
          f
      }
    }

    maybeCondition match {
      case None          => UpdatesIntervalsFilter.empty
      case Some(And(cs)) => cs.foldLeft(UpdatesIntervalsFilter.empty)((f, c) => addSimpleCondition(f, c))
      case Some(c)       => addSimpleCondition(UpdatesIntervalsFilter.empty, c)
    }
  }
}
