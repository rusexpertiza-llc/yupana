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
import org.yupana.core.sql.{ Parameter, TypedParameter }
import org.yupana.core.sql.parser._

import java.time.OffsetDateTime
import scala.util.matching.Regex

object UpdatesIntervalsProvider extends StrictLogging {
  import org.yupana.core.model.UpdateInterval._

  implicit class CaseInsensitiveRegex(val sc: StringContext) extends AnyVal {
    def ci: Regex = ("(?i)" + sc.parts.mkString).r
  }

  def handleGetUpdatesIntervals(
      flatQueryEngine: FlatQueryEngine,
      maybeCondition: Option[SqlExpr],
      parameters: Map[Int, Parameter]
  ): Either[String, Result] = {

    createFilter(maybeCondition, parameters).map { filter =>
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

  def createFilter(
      maybeCondition: Option[SqlExpr],
      params: Map[Int, Parameter] = Map.empty
  ): Either[String, UpdatesIntervalsFilter] = {
    def addSimpleCondition(f: UpdatesIntervalsFilter, c: SqlExpr): Either[String, UpdatesIntervalsFilter] = {
      c match {
        case Eq(FieldName(ci"table"), Constant(x)) => getTyped[String](x).map(s => f.withTableName(s))
        case Eq(Constant(x), FieldName(ci"table")) => getTyped[String](x).map(s => f.withTableName(s))
        case BetweenCondition(FieldName(ci"updated_at"), from, to) =>
          for {
            fromTime <- getTyped[Time](from)
            toTime <- getTyped[Time](to)
          } yield f.withUpdatedAfter(fromTime.toDateTime).withUpdatedBefore(toTime.toDateTime)
        case BetweenCondition(FieldName(ci"recalculated_at"), from, to) =>
          for {
            fromTime <- getTyped[Time](from)
            toTime <- getTyped[Time](to)
          } yield f.withRecalculatedAfter(fromTime.toDateTime).withRecalculatedBefore(toTime.toDateTime)
        case Ge(FieldName(ci"recalculated_at"), Constant(x)) =>
          getTyped[Time](x).map(s => f.withRecalculatedAfter(s.toDateTime))
        case Eq(FieldName(ci"updated_by"), Constant(x)) => getTyped[String](x).map(s => f.withBy(s))
        case Eq(Constant(x), FieldName(ci"updated_by")) => getTyped[String](x).map(s => f.withBy(s))
        case c                                          => Left(s"Unsupported condition: $c")
      }
    }

    def getTyped[T](value: Value)(implicit t: DataType.Aux[T]): Either[String, T] = {
      value match {
        case tv @ TypedValue(v) if tv.dataType == t => Right(v.asInstanceOf[T])
        case Placeholder(id) =>
          params.get(id).toRight(s"Parameter #$id is not defined").flatMap {
            case tv @ TypedParameter(v) if tv.dataType == t => Right(v.asInstanceOf[T])
            case x                                          => Left(s"Got $x for parameter #$id, but $t is required")
          }
        case x => Left(s"Got $x but $t is required")
      }
    }

    maybeCondition match {
      case None => Right(UpdatesIntervalsFilter.empty)
      case Some(And(cs)) =>
        cs.foldLeft(Right(UpdatesIntervalsFilter.empty): Either[String, UpdatesIntervalsFilter])((filter, c) =>
          filter.flatMap(f => addSimpleCondition(f, c))
        )
      case Some(c) => addSimpleCondition(UpdatesIntervalsFilter.empty, c)
    }
  }
}
