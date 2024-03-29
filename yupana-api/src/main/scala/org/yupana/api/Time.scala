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

package org.yupana.api

import org.yupana.api.utils.DimOrdering

import java.time.{ Instant, LocalDateTime, OffsetDateTime, ZoneOffset }

/**
  * Simple time value implementation.
  * @param millis epoch milliseconds in UTC.
  */
case class Time(millis: Long) extends Ordered[Time] {
  def toLocalDateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC)
  def toDateTime: OffsetDateTime = Instant.ofEpochMilli(millis).atOffset(ZoneOffset.UTC)

  override def toString: String = toDateTime.toString

  override def compare(that: Time): Int = this.millis.compare(that.millis)
}

object Time {
  implicit val dimOrdering: DimOrdering[Time] = DimOrdering.fromCmp(Ordering.by[Time, Long](_.millis).compare)

  implicit class TimeOps(t: Time) {
    def plus(value: Long): Time = t.copy(t.millis + value)
    def minus(value: Long): Time = t.copy(t.millis - value)
  }

  def apply(localDateTime: LocalDateTime): Time = new Time(localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli)
  def apply(dateTime: OffsetDateTime): Time = new Time(dateTime.toInstant.toEpochMilli)
}
