package org.yupana.api

import org.joda.time.{DateTime, DateTimeZone, LocalDateTime}

case class Time(millis: Long) {
  def toLocalDateTime: LocalDateTime = new LocalDateTime(millis, DateTimeZone.UTC)
  def toDateTime: DateTime = new DateTime(millis, DateTimeZone.UTC)

  override def toString: String = s"Time(${toDateTime.toString})"
}

object Time {
  implicit val ordering: Ordering[Time] = Ordering.by(_.millis)

  def apply(localDateTime: LocalDateTime): Time = new Time(localDateTime.toDateTime(DateTimeZone.UTC).getMillis)
  def apply(dateTime: DateTime): Time = new Time(dateTime.getMillis)
}
