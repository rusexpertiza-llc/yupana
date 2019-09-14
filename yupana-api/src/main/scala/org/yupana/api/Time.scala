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

import org.joda.time.{DateTime, DateTimeZone, LocalDateTime}

/**
  * Simple time value implementation.
  * @param millis epoch milliseconds in UTC.
  */
case class Time(millis: Long) {
  def toLocalDateTime: LocalDateTime = new LocalDateTime(millis, DateTimeZone.UTC)
  def toDateTime: DateTime = new DateTime(millis, DateTimeZone.UTC)

  override def toString: String = toDateTime.toString
}

object Time {
  implicit val ordering: Ordering[Time] = Ordering.by(_.millis)

  def apply(localDateTime: LocalDateTime): Time = new Time(localDateTime.toDateTime(DateTimeZone.UTC).getMillis)
  def apply(dateTime: DateTime): Time = new Time(dateTime.getMillis)
}
