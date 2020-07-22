package org.yupana.core.sql

import fastparse._
import NoWhitespace._
import org.joda.time.{ LocalDateTime, Period }
import org.yupana.api.Time
import org.yupana.api.query.{ ConstantExpr, Expression, PlaceholderExpr }

object NewValueParser {
  private def wsp[_: P] = P(CharsWhileIn(" \t", 0))

  private def timestampWord[_: P] = P(IgnoreCase("TIMESTAMP"))
  private def tsWord[_: P] = P(IgnoreCase("TS"))
  private def intervalWord[_: P] = P(IgnoreCase("INTERVAL"))

  private def toWord[_: P] = P(IgnoreCase("TO"))

//  def placeholder[_: P]: P[PlaceholderExpr.type] = P("?").map(_ => PlaceholderExpr())

  private def digit[_: P] = P(CharIn("0-9").!)

  private def digits[_: P]: P[String] = P(CharsWhileIn("0-9").!)

  def intNumber[_: P]: P[Int] = P(digits).map(_.toInt)

  def longNumber[_: P]: P[Long] = P(digits).map(_.toLong)

  def number[_: P]: P[BigDecimal] = P(digits ~ ("." ~ digits).!.?).map {
    case (x, y) => BigDecimal(x + y.getOrElse(""))
  }

  private def stringCharacter[_: P] = CharPred(c => c != '\'' && CharPredicates.isPrintableChar(c)).!
  private def escapedCharacter[_: P] = P("\\" ~/ CharIn("'\\\\").!)

  def string[_: P]: P[String] = P("'" ~/ (escapedCharacter | stringCharacter).rep ~ "'").map(_.mkString)

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

  def dateAndTime[_: P]: P[LocalDateTime] = P(date ~/ (" " ~ time).?).map {
    case (y, m, d, None)                  => new LocalDateTime(y, m, d, 0, 0, 0, 0)
    case (y, m, d, Some((h, mm, ss, ms))) => new LocalDateTime(y, m, d, h, mm, ss, ms)
  }

  def duration[_: P]: P[Period] = P("'" ~ (intNumber ~ " ").? ~ time ~ "'").map {
    case (d, (h, m, s, ms)) =>
      new Period(0, 0, 0, d.getOrElse(0), h, m, s, ms)
  }

  private def pgTimestamp[_: P]: P[LocalDateTime] = {
    P(timestampWord ~/ wsp ~ "'" ~ dateAndTime ~ "'")
  }
  private def msTimestamp[_: P]: P[LocalDateTime] = {
    P("{" ~ wsp ~ tsWord ~/ wsp ~ "'" ~ dateAndTime ~ "'" ~ wsp ~ "}")
  }

  def numericValue[_: P]: P[ConstantExpr] = P(number).map(ConstantExpr.apply)
  def stringValue[_: P]: P[ConstantExpr] = P(string).map(ConstantExpr.apply)
  def timestampValue[_: P]: P[ConstantExpr] = P(pgTimestamp | msTimestamp).map(ts => ConstantExpr(Time(ts)))

  def INTERVAL_PARTS[_: P]: List[IntervalPart] = List(
    IntervalPart(
      "SECOND",
      () =>
        (intNumber ~ ("." ~ millis).?).map {
          case (s, ms) => Period.seconds(s).plusMillis(ms.getOrElse(0))
        },
      () => P("")
    ),
    IntervalPart("MINUTE", () => intNumber.map(Period.minutes), () => P(":")),
    IntervalPart("HOUR", () => intNumber.map(Period.hours), () => P(":")),
    IntervalPart("DAY", () => intNumber.map(Period.days), () => P(" ")),
    IntervalPart("MONTH", () => intNumber.map(Period.months), () => P("-")),
    IntervalPart("YEAR", () => intNumber.map(Period.years), () => P("-"))
  )

  def singleFieldDuration[_: P]: P[Period] = {
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

//  def periodValue[_: P]: P[PeriodValue] = {
//    P(intervalWord ~/ wsp ~ (duration | singleFieldDuration)).map(PeriodValue)
//  }

  def value[_: P]: P[Expression] = P(numericValue | timestampValue | /*periodValue |*/ stringValue /* | placeholder*/ )

  case class IntervalPart(name: String, parser: () => P[Period], separator: () => P[Unit])
}
