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

import fastparse.WhitespaceApi
import fastparse.all._
import org.joda.time.{LocalDateTime, Period}
import org.yupana.core.sql.parser.ValueParser.IntervalPart

trait ValueParser {
  val white: WhitespaceApi.Wrapper = WhitespaceApi.Wrapper(NoTrace(StringIn("\r\n", "\n", " ", "\t")).rep)

  private val timestampWord = P(IgnoreCase("TIMESTAMP"))
  private val tsWord = P(IgnoreCase("TS"))
  private val intervalWord = P(IgnoreCase("INTERVAL"))

  private val toWord = P(IgnoreCase("TO"))

  val placeholder: Parser[Placeholder.type] = P("?").map(_ => Placeholder)

  private val digit = P(CharIn('0' to '9').!)

  private val digits: Parser[String] = P(CharsWhileIn('0' to '9').!)

  val intNumber: Parser[Int] = P(digits).map(_.toInt)

  val number: Parser[BigDecimal] = P(digits ~ ("." ~ digits).!.?).map { case (x, y) => BigDecimal(x + y.getOrElse("")) }

  private val stringCharacter = CharPred(c => c != '\'' && CharPredicates.isPrintableChar(c)).!
  private val escapedCharacter = P("\\" ~/ CharIn("'\\").!)

  val string: Parser[String] = P("'" ~/ (escapedCharacter | stringCharacter).rep ~ "'").map(_.mkString)

  private val year = P(digit.rep(exactly = 4).!.map(_.toInt))
  private val twoDigitInt = P(digit.rep(min = 1, max = 2).map(_.mkString.toInt))
  private val month = P(twoDigitInt.filter(x => x > 0 && x <= 12))
  private val day = P(twoDigitInt.filter(x => x > 0 && x <= 31))
  val date: Parser[(Int, Int, Int)] = P(year ~ "-" ~ month ~ "-" ~ day)

  private val hours = P(twoDigitInt.filter(x => x >= 0 && x <= 23))
  private val minutes = P(twoDigitInt.filter(x => x >= 0 && x <= 59))
  private val millis = P(digit.rep(min = 1, max = 3)
    .map(s => (s.mkString + ("0" * (3 - s.length))).toInt))

  val time: Parser[(Int, Int, Int, Int)] = P(hours ~ ":" ~ minutes ~ ":" ~ minutes ~ ("." ~ millis).?.map(_.getOrElse(0)))

  val dateAndTime: Parser[LocalDateTime] = P(date ~/ (" " ~ time).?).map {
    case (y, m, d, None) => new LocalDateTime(y, m, d, 0, 0, 0, 0)
    case (y, m, d, Some((h, mm, ss, ms))) => new LocalDateTime(y, m, d, h, mm, ss, ms)
  }

  val duration: Parser[Period] = P("'" ~ (intNumber ~ " ").? ~ time ~ "'").map { case (d, (h, m, s, ms)) =>
    new Period(0, 0, 0, d.getOrElse(0), h, m, s, ms)
  }

  private val pgTimestamp: Parser[LocalDateTime] = {
    import white._
    P(timestampWord ~/ "'" ~ dateAndTime ~ "'")
  }
  private val msTimestamp: Parser[LocalDateTime] = {
    import white._
    P("{" ~ tsWord ~/ "'" ~ dateAndTime ~ "'" ~ "}")
  }

  val numericValue: Parser[NumericValue] = P(number).map(NumericValue)
  val stringValue: Parser[StringValue] = P(string).map(StringValue)
  val timestampValue: Parser[TimestampValue] = P(pgTimestamp | msTimestamp).map(TimestampValue.apply)

  val INTERVAL_PARTS: List[IntervalPart] = List(
    IntervalPart("SECOND", (intNumber ~ ("." ~ millis).?).map {
      case (s, ms) => Period.seconds(s).plusMillis(ms.getOrElse(0))
    }, P("")),
    IntervalPart("MINUTE", intNumber.map(Period.minutes), P(":")),
    IntervalPart("HOUR", intNumber.map(Period.hours), P(":")),
    IntervalPart("DAY", intNumber.map(Period.days), P(" ")),
    IntervalPart("MONTH", intNumber.map(Period.months), P("-")),
    IntervalPart("YEAR", intNumber.map(Period.years), P("-"))
  )

  val singleFieldDuration: Parser[Period] = {
    val variants = INTERVAL_PARTS.tails.flatMap(_.inits).toList

    val parsers = variants.flatMap {
      case v :: Nil => Some(P("'" ~ v.parser ~ "' " ~ IgnoreCase(v.name)).opaque(v.name))

      case v :: vs =>
        val p = vs.map(p => P(p.parser ~ p.separator))
          .reduceRight((a, b) => (b ~ a).map { case (x, y) => x plus y})
        Some(P("'" ~ p ~ v.parser ~ "' " ~ IgnoreCase(vs.last.name) ~ " " ~ toWord ~ " " ~ IgnoreCase(v.name))
          .map { case (x, y) => x plus y }
          .opaque(s"${vs.last.name} TO ${v.name}"))

      case Nil => None
    }

    parsers.reduceLeft(_ | _)
  }


  val periodValue: Parser[PeriodValue] = {
    import white._
    P(intervalWord ~/ (duration | singleFieldDuration)).map(PeriodValue)
  }

  val value: Parser[Value] = P(numericValue | timestampValue  | periodValue | stringValue | placeholder)
}

object ValueParser {
  case class IntervalPart(name: String, parser: Parser[Period], separator: Parser[Unit])
}
