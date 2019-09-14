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

package org.yupana.core.sql.parser

import org.joda.time.{DateTimeZone, LocalDateTime, Period}

sealed trait Value {
  def asString: String
}

case object Placeholder extends Value {
  override def asString: String = throw new IllegalStateException("asString called on Placeholder")
}

case class NumericValue(value: BigDecimal) extends Value {
  override def asString: String = value.toString
}

case class StringValue(value: String) extends Value {
  override def asString: String = value
}

case class TimestampValue(value: LocalDateTime) extends Value {
  override def asString: String = value.toString
}

object TimestampValue {
  def apply(millis: Long): TimestampValue = new TimestampValue(new LocalDateTime(millis, DateTimeZone.UTC))
}

case class PeriodValue(value: Period) extends Value {
  override def asString: String = value.toString
}
