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

import java.time.DayOfWeek
import java.time.temporal.{ ChronoUnit, TemporalAdjusters }

class ConstantCalculator(tokenizer: Tokenizer) extends Serializable {

  def evaluateConstant[T](expr: Expression[T]): T = {
    assert(expr.kind == Const)

    import ExpressionCalculator.truncateTime

    expr match {
      case ConstantExpr(x) => x

      case ConditionExpr(condition, positive, negative) =>
        val x = evaluateConstant(condition)
        if (x) {
          evaluateConstant(positive)
        } else {
          evaluateConstant(negative)
        }

      case TruncYearExpr(e)   => evaluateUnary(e)(truncateTime(TemporalAdjusters.firstDayOfYear()))
      case TruncMonthExpr(e)  => evaluateUnary(e)(truncateTime(TemporalAdjusters.firstDayOfMonth()))
      case TruncWeekExpr(e)   => evaluateUnary(e)(truncateTime(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY)))
      case TruncDayExpr(e)    => evaluateUnary(e)(truncateTime(ChronoUnit.DAYS))
      case TruncHourExpr(e)   => evaluateUnary(e)(truncateTime(ChronoUnit.HOURS))
      case TruncMinuteExpr(e) => evaluateUnary(e)(truncateTime(ChronoUnit.MINUTES))
      case TruncSecondExpr(e) => evaluateUnary(e)(truncateTime(ChronoUnit.SECONDS))

      case ExtractYearExpr(e)   => evaluateUnary(e)(_.toLocalDateTime.getYear)
      case ExtractMonthExpr(e)  => evaluateUnary(e)(_.toLocalDateTime.getMonthValue)
      case ExtractDayExpr(e)    => evaluateUnary(e)(_.toLocalDateTime.getDayOfMonth)
      case ExtractHourExpr(e)   => evaluateUnary(e)(_.toLocalDateTime.getHour)
      case ExtractMinuteExpr(e) => evaluateUnary(e)(_.toLocalDateTime.getMinute)
      case ExtractSecondExpr(e) => evaluateUnary(e)(_.toLocalDateTime.getSecond)

      case p @ PlusExpr(a, b)    => evaluateBinary(a, b)(p.numeric.plus)
      case m @ MinusExpr(a, b)   => evaluateBinary(a, b)(m.numeric.minus)
      case t @ TimesExpr(a, b)   => evaluateBinary(a, b)(t.numeric.times)
      case d @ DivIntExpr(a, b)  => evaluateBinary(a, b)(d.integral.quot)
      case d @ DivFracExpr(a, b) => evaluateBinary(a, b)(d.fractional.div)

      case ConcatExpr(a, b) => evaluateBinary(a, b)(_ ++ _)

      case EqExpr(a, b)     => evaluateBinary(a, b)(_ == _)
      case NeqExpr(a, b)    => evaluateBinary(a, b)(_ != _)
      case e @ GtExpr(a, b) => evaluateBinary(a, b)(e.ordering.gt)
      case e @ LtExpr(a, b) => evaluateBinary(a, b)(e.ordering.lt)
      case e @ GeExpr(a, b) => evaluateBinary(a, b)(e.ordering.gteq)
      case e @ LeExpr(a, b) => evaluateBinary(a, b)(e.ordering.lteq)

      case IsNullExpr(e)    => evaluateConstant(e) == null
      case IsNotNullExpr(e) => evaluateConstant(e) != null

      case TypeConvertExpr(tc, e) => tc.convert(evaluateConstant(e))

      case InExpr(e, vs) => vs contains evaluateConstant(e)

      case NotInExpr(e, vs) =>
        val eValue = evaluateConstant(e)
        eValue != null && !vs.contains(eValue)

      case AndExpr(cs) =>
        val executed = cs.map(c => evaluateConstant(c))
        executed.reduce((a, b) => a && b)

      case OrExpr(cs) =>
        val executed = cs.map(c => evaluateConstant(c))
        executed.reduce((a, b) => a || b)

      case TupleExpr(e1, e2) => (evaluateConstant(e1), evaluateConstant(e2))

      case LowerExpr(e)  => evaluateUnary(e)(_.toLowerCase)
      case UpperExpr(e)  => evaluateUnary(e)(_.toUpperCase)
      case LengthExpr(e) => evaluateUnary(e)(_.length)
      case SplitExpr(e)  => evaluateUnary(e)(s => ExpressionCalculator.splitBy(s, !_.isLetterOrDigit).toSeq)
      case TokensExpr(e) => evaluateUnary(e)(s => tokenizer.transliteratedTokens(s))

      case a @ AbsExpr(e)        => evaluateUnary(e)(a.num.abs)
      case u @ UnaryMinusExpr(e) => evaluateUnary(e)(u.num.negate)

      case NotExpr(e) => evaluateUnary(e)(x => !x)

      case TimeMinusExpr(a, b) => evaluateBinary(a, b)((x, y) => math.abs(x.millis - y.millis))
      case TimeMinusPeriodExpr(a, b) =>
        evaluateBinary(a, b)((t, p) => Time(t.toDateTime.minus(p).toInstant.toEpochMilli))
      case TimePlusPeriodExpr(a, b)   => evaluateBinary(a, b)((t, p) => Time(t.toDateTime.plus(p).toInstant.toEpochMilli))
      case PeriodPlusPeriodExpr(a, b) => evaluateBinary(a, b)((x, y) => x plus y)

      case ArrayTokensExpr(e)   => evaluateUnary(e)(a => a.flatMap(s => tokenizer.transliteratedTokens(s)))
      case ArrayLengthExpr(e)   => evaluateUnary(e)(_.length)
      case ArrayToStringExpr(e) => evaluateUnary(e)(_.mkString(", "))

      case ContainsExpr(a, b)     => evaluateBinary(a, b)(_ contains _)
      case ContainsAllExpr(a, b)  => evaluateBinary(a, b)((x, y) => y.forall(x.contains))
      case ContainsAnyExpr(a, b)  => evaluateBinary(a, b)((x, y) => y.exists(x.contains))
      case ContainsSameExpr(a, b) => evaluateBinary(a, b)((x, y) => x.size == y.size && x.toSet == y.toSet)

      case ArrayExpr(es) => es.map(e => evaluateConstant(e))

      case x => throw new IllegalArgumentException(s"Expression is not constant $x")
    }
  }

  private def evaluateUnary[A, O](e: Expression[A])(f: A => O): O = {
    val ev = evaluateConstant(e)
    if (ev != null) f(ev) else null.asInstanceOf[O]
  }

  private def evaluateBinary[A, B, O](a: Expression[A], b: Expression[B])(f: (A, B) => O): O = {
    val left = evaluateConstant(a)
    val right = evaluateConstant(b)
    if (left != null && right != null) {
      f(left, right)
    } else {
      null.asInstanceOf[O]
    }
  }
}
