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

  private def wsp[$: P] = P(CharsWhileIn(" \t", 0))

  private def timestampWord[$: P] = P(IgnoreCase("TIMESTAMP"))
  private def tsWord[$: P] = P(IgnoreCase("TS"))
  private def intervalWord[$: P] = P(IgnoreCase("INTERVAL"))

  private def toWord[$: P] = P(IgnoreCase("TO"))

  private def nullWord[$: P] = P(IgnoreCase("NULL"))

  private def trueConst[$: P]: P[Boolean] = P(IgnoreCase("TRUE")).map(_ => true)
  private def falseConst[$: P]: P[Boolean] = P(IgnoreCase("FALSE")).map(_ => false)

  def placeholder[$: P]: P[Placeholder] = {
    val p = P("?")
    val idx = p.misc.getOrElse(PLACEHOLDER_ID, 1).asInstanceOf[Int]

    if (p.isSuccess) {
      p.misc.put(PLACEHOLDER_ID, idx + 1)
      p.freshSuccess(Placeholder(idx))
    } else {
      p.freshFailure()
    }
  }

  private def digit[$: P] = P(CharIn("0-9").!)

  private def digits[$: P]: P[String] = P(CharsWhileIn("0-9").!)

  def intNumber[$: P]: P[Int] = P(digits).map(_.toInt)

  def number[$: P]: P[BigDecimal] = P("-".!.? ~ digits ~ ("." ~ digits).!.?).map {
    case (m, x, y) => BigDecimal(m.getOrElse("") + x + y.getOrElse(""))
  }

  private def stringCharacter[$: P] = CharPred(c => c != '\'' && CharPredicates.isPrintableChar(c)).!
  def escapedCharacter[$: P] = P("\\" ~/ CharIn("'\\\\nrt").!).map {
    case "n" => "\n"
    case "r" => "\r"
    case "t" => "\t"
    case x   => x
  }

  def string[$: P]: P[String] = P("'" ~/ (escapedCharacter | stringCharacter).rep ~ "'").map(_.mkString)
  def boolean[$: P]: P[Boolean] = P(trueConst | falseConst)

  private def year[$: P] = P(digit.rep(exactly = 4).!.map(_.toInt))
  private def twoDigitInt[$: P] = P(digit.rep(min = 1, max = 2).map(_.mkString.toInt))
  private def month[$: P] = P(twoDigitInt.filter(x => x > 0 && x <= 12))
  private def day[$: P] = P(twoDigitInt.filter(x => x > 0 && x <= 31))
  def date[$: P]: P[(Int, Int, Int)] = P(year ~ "-" ~ month ~ "-" ~ day)

  private def hours[$: P] = P(twoDigitInt.filter(x => x >= 0 && x <= 23))
  private def minutes[$: P] = P(twoDigitInt.filter(x => x >= 0 && x <= 59))
  private def millis[$: P] =
    P(
      digit
        .rep(min = 1, max = 3)
        .map(s => (s.mkString + ("0" * (3 - s.length))).toInt)
    )

  def time[$: P]: P[(Int, Int, Int, Int)] =
    P(hours ~ ":" ~ minutes ~ ":" ~ minutes ~ ("." ~ millis).?.map(_.getOrElse(0)))

  def dateAndTime[$: P]: P[OffsetDateTime] = P(date ~/ (" " ~ time).?).map {
    case (y, m, d, None)                  => OffsetDateTime.of(y, m, d, 0, 0, 0, 0, ZoneOffset.UTC)
    case (y, m, d, Some((h, mm, ss, ms))) => OffsetDateTime.of(y, m, d, h, mm, ss, ms, ZoneOffset.UTC)
  }

  def duration[$: P]: P[PeriodDuration] = P("'" ~ (intNumber ~ " ").? ~ time ~ "'").map {
    case (d, (h, m, s, ms)) =>
      PeriodDuration
        .of(Period.of(0, 0, d.getOrElse(0)), Duration.ofHours(h).plusMinutes(m).plusSeconds(s).plusMillis(ms))
  }

  private def pgTimestamp[$: P]: P[OffsetDateTime] = {
    P(timestampWord ~/ wsp ~ "'" ~ dateAndTime ~ "'")
  }
  private def msTimestamp[$: P]: P[OffsetDateTime] = {
    P("{" ~ wsp ~ tsWord ~/ wsp ~ "'" ~ dateAndTime ~ "'" ~ wsp ~ "}")
  }

  def numericValue[$: P]: P[NumericValue] = P(number).map(NumericValue)
  def stringValue[$: P]: P[StringValue] = P(string).map(StringValue)
  def booleanValue[$: P]: P[BooleanValue] = P(boolean).map(BooleanValue)
  def nullValue[$: P]: P[NullValue.type] = P(nullWord).map(_ => NullValue)
  def timestampValue[$: P]: P[TimestampValue] = P(pgTimestamp | msTimestamp).map(TimestampValue.apply)

  def INTERVAL_PARTS[$: P]: List[IntervalPart] = List(
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

  def singleFieldDuration[$: P]: P[PeriodDuration] = {
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

  def periodValue[$: P]: P[PeriodValue] = {
    P(intervalWord ~/ wsp ~ (duration | singleFieldDuration)).map(PeriodValue)
  }

  def tupleValue[$: P]: P[TupleValue] =
    P("(" ~ wsp ~ value ~ wsp ~ "," ~/ wsp ~ value ~/ wsp ~ ")").map(TupleValue.tupled)

  def value[$: P]: P[Value] = P(
    numericValue | timestampValue | periodValue | stringValue | booleanValue | nullValue | placeholder | tupleValue
  )

  case class IntervalPart(name: String, parser: () => P[PeriodDuration], separator: () => P[Unit])
}
