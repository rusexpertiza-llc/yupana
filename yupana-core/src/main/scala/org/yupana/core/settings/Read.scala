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

package org.yupana.core.settings

import java.time.format.{ DateTimeFormatter, DateTimeFormatterBuilder }
import java.time.temporal.ChronoField
import java.time.{ LocalDate, LocalDateTime, OffsetDateTime, Period }
import scala.concurrent.duration.Duration

trait Read[+T] {
  def read(s: String): T
}

object Read {

  implicit val stringRead: Read[String] = _.trim
  implicit val intRead: Read[Int] = _.toInt
  implicit val longRead: Read[Long] = _.toLong
  implicit val doubleRead: Read[Double] = _.toDouble
  implicit val booleanRead: Read[Boolean] = _.toBoolean

  implicit val bigDecimalRead: Read[BigDecimal] = (s: String) => BigDecimal(s)

  implicit val durationRead: Read[Duration] = Duration.apply(_)
  implicit val localDateRead: Read[LocalDate] = LocalDate.parse(_)
  implicit val localDateTimeRead: Read[LocalDateTime] = LocalDateTime.parse(_, optionalTimeFormatter)
  implicit val dateTimeRead: Read[OffsetDateTime] = OffsetDateTime.parse(_)
  implicit val periodRead: Read[Period] = Period.parse(_)

  val optionalTimeFormatter: DateTimeFormatter = new DateTimeFormatterBuilder()
    .append(DateTimeFormatter.ISO_DATE)
    .optionalStart()
    .appendLiteral('T')
    .append(DateTimeFormatter.ISO_TIME)
    .optionalEnd()
    .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
    .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
    .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
    .toFormatter()

  private def stringToSeq(s: String): Seq[String] =
    s.split("(?<!\\\\),|\r?\n|\r").map(_.replaceAll("\\\\,", ",")).toSeq.map(_.trim)

  implicit def commaSeparatedSeqRead[R: Read]: Read[Seq[R]] =
    stringToSeq(_).map(implicitly[Read[R]].read)

  implicit def commaSeparatedSetRead[R: Read]: Read[Set[R]] =
    stringToSeq(_).map(implicitly[Read[R]].read).toSet
}
