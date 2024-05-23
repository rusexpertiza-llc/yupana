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

package org.yupana.core

import org.yupana.api.Time
import org.yupana.api.query._
import org.yupana.api.utils.Tokenizer
import org.yupana.core.jit.ExpressionCalculator

import java.time.DayOfWeek
import java.time.temporal.{ ChronoUnit, TemporalAdjusters }

class ConstantCalculator(tokenizer: Tokenizer) extends Serializable {

  def evaluateConstant[T](
      expr: Expression[T],
      startTime: Option[Time] = None,
      params: IndexedSeq[Any] = IndexedSeq.empty
  ): T = {
    assert(expr.kind == Const)

    import ExpressionCalculator.truncateTime
    import ExpressionCalculator.truncateTimeBy

    expr match {
      case ConstantExpr(x) => x
      case NullExpr(_)     => null.asInstanceOf[T]
      case TrueExpr        => true
      case FalseExpr       => false
      case PlaceholderExpr(id, _) =>
        if (id < params.length + 1) params(id - 1).asInstanceOf[T]
        else throw new IllegalStateException(s"Parameter #$id value is not defined")
      case UntypedPlaceholderExpr(id) =>
        throw new IllegalStateException(s"Untyped placeholder #$id passed to calculator")

      case ConditionExpr(condition, positive, negative) =>
        val x = evaluateConstant(condition, startTime, params)
        if (x) {
          evaluateConstant(positive, startTime, params)
        } else {
          evaluateConstant(negative, startTime, params)
        }

      case TruncYearExpr(e) => evaluateUnary(e, startTime, params)(truncateTime(TemporalAdjusters.firstDayOfYear))
      case TruncQuarterExpr(e) =>
        evaluateUnary(e, startTime, params)(
          truncateTimeBy(dTime =>
            dTime.`with`(TemporalAdjusters.firstDayOfMonth).withMonth(dTime.getMonth.firstMonthOfQuarter.getValue)
          )
        )
      case TruncMonthExpr(e) => evaluateUnary(e, startTime, params)(truncateTime(TemporalAdjusters.firstDayOfMonth))
      case TruncWeekExpr(e) =>
        evaluateUnary(e, startTime, params)(truncateTime(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY)))
      case TruncDayExpr(e)    => evaluateUnary(e, startTime, params)(truncateTime(ChronoUnit.DAYS))
      case TruncHourExpr(e)   => evaluateUnary(e, startTime, params)(truncateTime(ChronoUnit.HOURS))
      case TruncMinuteExpr(e) => evaluateUnary(e, startTime, params)(truncateTime(ChronoUnit.MINUTES))
      case TruncSecondExpr(e) => evaluateUnary(e, startTime, params)(truncateTime(ChronoUnit.SECONDS))

      case ExtractYearExpr(e) => evaluateUnary(e, startTime, params)(_.toLocalDateTime.getYear)
      case ExtractQuarterExpr(e) =>
        evaluateUnary(e, startTime, params)(t => 1 + (t.toLocalDateTime.getMonth.getValue - 1) / 3)
      case ExtractMonthExpr(e)  => evaluateUnary(e, startTime, params)(_.toLocalDateTime.getMonthValue)
      case ExtractDayExpr(e)    => evaluateUnary(e, startTime, params)(_.toLocalDateTime.getDayOfMonth)
      case ExtractHourExpr(e)   => evaluateUnary(e, startTime, params)(_.toLocalDateTime.getHour)
      case ExtractMinuteExpr(e) => evaluateUnary(e, startTime, params)(_.toLocalDateTime.getMinute)
      case ExtractSecondExpr(e) => evaluateUnary(e, startTime, params)(_.toLocalDateTime.getSecond)

      case p @ PlusExpr(a, b)    => evaluateBinary(a, b, startTime, params)(p.numeric.plus)
      case m @ MinusExpr(a, b)   => evaluateBinary(a, b, startTime, params)(m.numeric.minus)
      case t @ TimesExpr(a, b)   => evaluateBinary(a, b, startTime, params)(t.numeric.times)
      case d @ DivIntExpr(a, b)  => evaluateBinary(a, b, startTime, params)(d.integral.quot)
      case d @ DivFracExpr(a, b) => evaluateBinary(a, b, startTime, params)(d.fractional.div)

      case ConcatExpr(a, b) => evaluateBinary(a, b, startTime, params)(_ ++ _)

      case EqExpr(a, b)     => evaluateBinary(a, b, startTime, params)(_ == _)
      case NeqExpr(a, b)    => evaluateBinary(a, b, startTime, params)(_ != _)
      case e @ GtExpr(a, b) => evaluateBinary(a, b, startTime, params)(e.ordering.gt)
      case e @ LtExpr(a, b) => evaluateBinary(a, b, startTime, params)(e.ordering.lt)
      case e @ GeExpr(a, b) => evaluateBinary(a, b, startTime, params)(e.ordering.gteq)
      case e @ LeExpr(a, b) => evaluateBinary(a, b, startTime, params)(e.ordering.lteq)

      case IsNullExpr(e)    => evaluateConstant(e, startTime, params) == null
      case IsNotNullExpr(e) => evaluateConstant(e, startTime, params) != null

      case Double2BigDecimalExpr(e) => evaluateUnary(e, startTime, params)(BigDecimal.valueOf)
      case BigDecimal2DoubleExpr(e) => evaluateUnary(e, startTime, params)(_.toDouble)
      case Long2BigDecimalExpr(e)   => evaluateUnary(e, startTime, params)(BigDecimal.valueOf)
      case Long2DoubleExpr(e)       => evaluateUnary(e, startTime, params)(_.toDouble)
      case Int2LongExpr(e)          => evaluateUnary(e, startTime, params)(_.toLong)
      case Int2BigDecimalExpr(e)    => evaluateUnary(e, startTime, params)(BigDecimal.valueOf(_))
      case Int2DoubleExpr(e)        => evaluateUnary(e, startTime, params)(_.toDouble)
      case Short2IntExpr(e)         => evaluateUnary(e, startTime, params)(_.toInt)
      case Short2LongExpr(e)        => evaluateUnary(e, startTime, params)(_.toLong)
      case Short2BigDecimalExpr(e)  => evaluateUnary(e, startTime, params)(BigDecimal.valueOf(_))
      case Short2DoubleExpr(e)      => evaluateUnary(e, startTime, params)(_.toDouble)
      case Byte2ShortExpr(e)        => evaluateUnary(e, startTime, params)(_.toShort)
      case Byte2IntExpr(e)          => evaluateUnary(e, startTime, params)(_.toInt)
      case Byte2LongExpr(e)         => evaluateUnary(e, startTime, params)(_.toLong)
      case Byte2BigDecimalExpr(e)   => evaluateUnary(e, startTime, params)(BigDecimal.valueOf(_))
      case Byte2DoubleExpr(e)       => evaluateUnary(e, startTime, params)(_.toDouble)
      case ToStringExpr(e)          => evaluateUnary(e, startTime, params)(_.toString)

      case InExpr(e, vs) => vs contains evaluateConstant(e, startTime, params)

      case NotInExpr(e, vs) =>
        val eValue = evaluateConstant(e, startTime, params)
        eValue != null && !vs.contains(eValue)

      case AndExpr(cs) =>
        val executed = cs.map(c => evaluateConstant(c, startTime, params))
        executed.reduce((a, b) => a && b)

      case OrExpr(cs) =>
        val executed = cs.map(c => evaluateConstant(c, startTime, params))
        executed.reduce((a, b) => a || b)

      case TupleExpr(e1, e2) => (evaluateConstant(e1, startTime, params), evaluateConstant(e2, startTime, params))

      case LowerExpr(e)  => evaluateUnary(e, startTime, params)(_.toLowerCase)
      case UpperExpr(e)  => evaluateUnary(e, startTime, params)(_.toUpperCase)
      case LengthExpr(e) => evaluateUnary(e, startTime, params)(_.length)
      case SplitExpr(e) =>
        evaluateUnary(e, startTime, params)(s => ExpressionCalculator.splitBy(s, !_.isLetterOrDigit).toSeq)
      case TokensExpr(e) => evaluateUnary(e, startTime, params)(s => tokenizer.transliteratedTokens(s))

      case a @ AbsExpr(e)        => evaluateUnary(e, startTime, params)(a.num.abs)
      case u @ UnaryMinusExpr(e) => evaluateUnary(e, startTime, params)(u.num.negate)

      case NotExpr(e) => evaluateUnary(e, startTime, params)(x => !x)

      case TimeMinusExpr(a, b) => evaluateBinary(a, b, startTime, params)((x, y) => math.abs(x.millis - y.millis))
      case TimeMinusPeriodExpr(a, b) =>
        evaluateBinary(a, b, startTime, params)((t, p) => Time(t.toDateTime.minus(p).toInstant.toEpochMilli))
      case TimePlusPeriodExpr(a, b) =>
        evaluateBinary(a, b, startTime, params)((t, p) => Time(t.toDateTime.plus(p).toInstant.toEpochMilli))
      case PeriodPlusPeriodExpr(a, b) => evaluateBinary(a, b, startTime, params)((x, y) => x plus y)

      case ArrayTokensExpr(e) =>
        evaluateUnary(e, startTime, params)(a => a.flatMap(s => tokenizer.transliteratedTokens(s)))
      case ArrayLengthExpr(e)   => evaluateUnary(e, startTime, params)(_.length)
      case ArrayToStringExpr(e) => evaluateUnary(e, startTime, params)(_.mkString(", "))

      case ContainsExpr(a, b)    => evaluateBinary(a, b, startTime, params)(_ contains _)
      case ContainsAllExpr(a, b) => evaluateBinary(a, b, startTime, params)((x, y) => y.forall(x.contains))
      case ContainsAnyExpr(a, b) => evaluateBinary(a, b, startTime, params)((x, y) => y.exists(x.contains))
      case ContainsSameExpr(a, b) =>
        evaluateBinary(a, b, startTime, params)((x, y) => x.size == y.size && x.toSet == y.toSet)

      case ArrayExpr(es) => es.map(e => evaluateConstant(e, startTime, params))

      case MetricExpr(_) | DimensionExpr(_) | TimeExpr | LinkExpr(_, _) | DimIdInExpr(_, _) | DimIdNotInExpr(_, _) |
          DimensionIdExpr(_) | LagExpr(_) | _: AggregateExpr[_, _, _] | NowExpr =>
        startTime.getOrElse(throw new IllegalArgumentException(s"Expression is not constant $expr"))
    }
  }

  private def evaluateUnary[A, O](e: Expression[A], startTime: Option[Time], params: IndexedSeq[Any])(f: A => O): O = {
    val ev = evaluateConstant(e, startTime, params)
    if (ev != null) f(ev) else null.asInstanceOf[O]
  }

  private def evaluateBinary[A, B, O](
      a: Expression[A],
      b: Expression[B],
      startTime: Option[Time],
      params: IndexedSeq[Any]
  )(
      f: (A, B) => O
  ): O = {
    val left = evaluateConstant(a, startTime, params)
    val right = evaluateConstant(b, startTime, params)
    if (left != null && right != null) {
      f(left, right)
    } else {
      null.asInstanceOf[O]
    }
  }
}
