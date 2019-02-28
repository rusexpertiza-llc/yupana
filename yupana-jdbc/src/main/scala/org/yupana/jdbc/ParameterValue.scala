package org.yupana.jdbc

import org.joda.time.{DateTimeZone, LocalDateTime}

sealed trait ParameterValue

case class NumericValue(value: BigDecimal) extends ParameterValue

case class StringValue(value: String) extends ParameterValue

case class TimestampValue(value: LocalDateTime) extends ParameterValue {
  def millis: Long = value.toDateTime(DateTimeZone.UTC).getMillis
}
