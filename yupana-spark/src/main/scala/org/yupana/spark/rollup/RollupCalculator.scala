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

package org.yupana.spark.rollup

import org.joda.time.{ DateTimeFieldType, DateTimeZone, Interval, LocalDateTime }
import org.yupana.api.schema.Rollup

object RollupCalculator {

  val MIN_TIME = new LocalDateTime(2017, 1, 1, 0, 0, 0, 0) //нет смысла считать роллапы ранее 1 янв 2017 г.
  val MAX_TIME = LocalDateTime.now().plusYears(1)

  def rollupTotalInterval(from: Option[LocalDateTime], to: Option[LocalDateTime]): Interval =
    new Interval(
      from.getOrElse(MIN_TIME).toDateTime(DateTimeZone.UTC),
      to.getOrElse(MAX_TIME).toDateTime(DateTimeZone.UTC)
    )

  def concatAdjacent(intervals: Seq[Interval]): Seq[Interval] = {
    intervals.sortBy(_.getStartMillis).foldRight[List[Interval]](Nil) {
      case (interval, neighbor :: rest) if interval.getEndMillis == neighbor.getStartMillis =>
        new Interval(interval.getStart, neighbor.getEnd) :: rest
      case (interval, mergedIntervals) => interval :: mergedIntervals
    }
  }

  def findInvalid(intervals: Seq[Interval], invalidMarks: Seq[Long], recalcAfter: Long): Seq[Interval] = {

    concatAdjacent(
      intervals.filter(i =>
        i.getEndMillis > recalcAfter ||
          invalidMarks.exists(m => i.getStartMillis <= m && m < i.getEndMillis)
      )
    )
  }

  def sliceInterval(interval: Interval, field: DateTimeFieldType): Seq[Interval] = {
    val intervalStartRounded = interval.getStart.property(field).roundFloorCopy()
    if (intervalStartRounded.isBefore(interval.getEnd)) {
      val timestamps = Stream
        .from(0)
        .map(i => intervalStartRounded.property(field).addToCopy(i))
        .takeWhile(_.isBefore(interval.getEnd))
        .toList :+ interval.getEnd
      timestamps.sliding(2).toList map {
        case start :: end :: Nil => new Interval(start, end)
        case _ =>
          throw new IllegalArgumentException(
            s"Bad timestamps sequence: $timestamps, " +
              s"it should contain even number of memebers"
          )
      }
    } else {
      Seq.empty
    }
  }

  def calcLevels(rollups: Seq[Rollup]): Either[String, Seq[Seq[Rollup]]] = {
    def buildLevels(cur: Seq[Rollup], rest: Seq[Rollup]): Either[String, Seq[Seq[Rollup]]] = {
      if (rest.isEmpty) {
        Right(Seq(cur))
      } else {
        val (nc, nr) = rest.partition(r => cur.exists(_.toTable == r.fromTable))
        if (nc.nonEmpty) {
          buildLevels(nc, nr).right.map(ls => cur +: ls)
        } else Left(s"Don't know how to build rollups: $rest")
      }
    }

    val (topLevel, rest) = rollups.partition(r => !rollups.exists(_.toTable == r.fromTable))
    buildLevels(topLevel, rest)
  }

}
