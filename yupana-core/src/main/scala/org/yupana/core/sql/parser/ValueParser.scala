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

import fastparse._
import NoWhitespace._
import org.threeten.extra.PeriodDuration

import java.time.{ Duration, OffsetDateTime, Period, ZoneOffset }

object ValueParser {
  private val PLACEHOLDER_ID = "PLACEHOLDER_ID"

  private def wsp[_: P] = P(CharsWhileIn(" \t", 0))

  private def timestampWord[_: P] = P(IgnoreCase("TIMESTAMP"))
  private def tsWord[_: P] = P(IgnoreCase("TS"))
  private def intervalWord[_: P] = P(IgnoreCase("INTERVAL"))

  private def toWord[_: P] = P(IgnoreCase("TO"))

  def placeholder[_: P]: P[Placeholder] = {
    val p = P("?")
    val idx = p.misc.getOrElse(PLACEHOLDER_ID, 1).asInstanceOf[Int]

    if (p.isSuccess) {
      p.misc.put(PLACEHOLDER_ID, idx + 1)
      p.freshSuccess(Placeholder(idx))
    } else {
      p.freshFailure()
    }
  }

  private def digit[_: P] = P(CharIn("0-9").!)

  private def digits[_: P]: P[String] = P(CharsWhileIn("0-9").!)

  def intNumber[_: P]: P[Int] = P(digits).map(_.toInt)

  def number[_: P]: P[BigDecimal] = P("-".!.? ~ digits ~ ("." ~ digits).!.?).map {
    case (m, x, y) => BigDecimal(m.getOrElse("") + x + y.getOrElse(""))
  }

  private def stringCharacter[_: P] = CharPred(c => c != '\'' && CharPredicates.isPrintableChar(c)).!
  def escapedCharacter[_: P] = P("\\" ~/ CharIn("'\\\\nrt").!).map {
    case "n" => "\n"
    case "r" => "\r"
    case "t" => "\t"
    case x   => x
  }

  def string[_: P]: P[String] = P("'" ~/ (escapedCharacter | stringCharacter).rep ~ "'").map(_.mkString)
  def boolean[_: P]: P[Boolean] = string.map(_.toBoolean)

  private def year[_: P] = P(digit.rep(exactly = 4).!.map(_.toInt))
  private def twoDigitInt[_: P] = P(digit.rep(min = 1, max = 2).map(_.mkString.toInt))
  private def month[_: P] = P(twoDigitInt.filter(x => x > 0 && x <= 12))
  private def day[_: P] = P(twoDigitInt.filter(x => x > 0 && x <= 31))
  def date[_: P]: P[(Int, Int, Int)] = P(year ~ "-" ~ month ~ "-" ~ day)

  private def hours[_: P] = P(twoDigitInt.filter(x => x >= 0 && x <= 23))
  private def minutes[_: P] = P(twoDigitInt.filter(x => x >= 0 && x <= 59))
  private def millis[_: P] =
    P(
      digit
        .rep(min = 1, max = 3)
        .map(s => (s.mkString + ("0" * (3 - s.length))).toInt)
    )

  def time[_: P]: P[(Int, Int, Int, Int)] =
    P(hours ~ ":" ~ minutes ~ ":" ~ minutes ~ ("." ~ millis).?.map(_.getOrElse(0)))

  def dateAndTime[_: P]: P[OffsetDateTime] = P(date ~/ (" " ~ time).?).map {
    case (y, m, d, None)                  => OffsetDateTime.of(y, m, d, 0, 0, 0, 0, ZoneOffset.UTC)
    case (y, m, d, Some((h, mm, ss, ms))) => OffsetDateTime.of(y, m, d, h, mm, ss, ms, ZoneOffset.UTC)
  }

  def duration[_: P]: P[PeriodDuration] = P("'" ~ (intNumber ~ " ").? ~ time ~ "'").map {
    case (d, (h, m, s, ms)) =>
      PeriodDuration
        .of(Period.of(0, 0, d.getOrElse(0)), Duration.ofHours(h).plusMinutes(m).plusSeconds(s).plusMillis(ms))
  }

  private def pgTimestamp[_: P]: P[OffsetDateTime] = {
    P(timestampWord ~/ wsp ~ "'" ~ dateAndTime ~ "'")
  }
  private def msTimestamp[_: P]: P[OffsetDateTime] = {
    P("{" ~ wsp ~ tsWord ~/ wsp ~ "'" ~ dateAndTime ~ "'" ~ wsp ~ "}")
  }

  def numericValue[_: P]: P[NumericValue] = P(number).map(NumericValue)
  def stringValue[_: P]: P[StringValue] = P(string).map(StringValue)
  def timestampValue[_: P]: P[TimestampValue] = P(pgTimestamp | msTimestamp).map(TimestampValue.apply)

  def INTERVAL_PARTS[_: P]: List[IntervalPart] = List(
    IntervalPart(
      "SECOND",
      () =>
        (intNumber ~ ("." ~ millis).?).map {
          case (s, ms) => PeriodDuration.of(Duration.ofSeconds(s).plusMillis(ms.getOrElse(0).toLong))
        },
      () => P("")
    ),
    IntervalPart("MINUTE", () => intNumber.map(m => PeriodDuration.of(Duration.ofMinutes(m))), () => P(":")),
    IntervalPart("HOUR", () => intNumber.map(h => PeriodDuration.of(Duration.ofHours(h))), () => P(":")),
    IntervalPart("DAY", () => intNumber.map(d => PeriodDuration.of(Period.ofDays(d))), () => P(" ")),
    IntervalPart("MONTH", () => intNumber.map(m => PeriodDuration.of(Period.ofMonths(m))), () => P("-")),
    IntervalPart("YEAR", () => intNumber.map(y => PeriodDuration.of(Period.ofYears(y))), () => P("-"))
  )

  def singleFieldDuration[_: P]: P[PeriodDuration] = {
    val variants = INTERVAL_PARTS.tails.flatMap(_.inits).toList

    val parsers = variants.flatMap {
      case v :: Nil => Some(() => P("'" ~ v.parser() ~ "' " ~ IgnoreCase(v.name)).opaque(v.name))

      case v :: vs =>
        val p = vs
          .map(p => () => p.parser() ~ p.separator())
          .reduceRight((a, b) => () => P(b() ~ a()).map { case (x, y) => x plus y })
        Some(() =>
          P("'" ~ p() ~ v.parser() ~ "' " ~ IgnoreCase(vs.last.name) ~ " " ~ toWord ~ " " ~ IgnoreCase(v.name))
            .map { case (x, y) => x plus y }
            .opaque(s"${vs.last.name} TO ${v.name}")
        )

      case Nil => None
    }

    parsers.reduceLeft((x, y) => () => x() | y())()
  }

  def periodValue[_: P]: P[PeriodValue] = {
    P(intervalWord ~/ wsp ~ (duration | singleFieldDuration)).map(PeriodValue)
  }

  def value[_: P]: P[Value] = P(numericValue | timestampValue | periodValue | stringValue | placeholder)

  case class IntervalPart(name: String, parser: () => P[PeriodDuration], separator: () => P[Unit])
}
