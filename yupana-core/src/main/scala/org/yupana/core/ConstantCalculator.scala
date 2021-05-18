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

import org.joda.time.DateTimeFieldType
import org.yupana.api.Time
import org.yupana.api.query._
import org.yupana.api.utils.Tokenizer

class ConstantCalculator(tokenizer: Tokenizer) extends Serializable {

  def evaluateConstant[T](expr: Expression[T]): T = {
    assert(expr.kind == Const)
    eval(expr)
  }

  private def eval[T](expr: Expression[T]): T = {
    import ExpressionCalculator.truncateTime

    expr match {
      case ConstantExpr(x) => x //.asInstanceOf[expr.Out]

      case TimeExpr             => null.asInstanceOf[T]
      case DimensionExpr(_)     => null.asInstanceOf[T]
      case DimensionIdExpr(_)   => null.asInstanceOf[T]
      case MetricExpr(_)        => null.asInstanceOf[T]
      case LinkExpr(_, _)       => null.asInstanceOf[T]
      case DimIdInExpr(_, _)    => null.asInstanceOf[T]
      case DimIdNotInExpr(_, _) => null.asInstanceOf[T]

      // GENERALLY ae.dataType != ae.expr.dataType, but we don't care here
      case ae: AggregateExpr[_, _, _]   => eval(ae.expr).asInstanceOf[T]
      case we: WindowFunctionExpr[_, _] => eval(we.expr).asInstanceOf[T]

      case ConditionExpr(condition, positive, negative) =>
        val x = eval(condition)
        if (x) {
          eval(positive)
        } else {
          eval(negative)
        }

      case TruncYearExpr(e)   => evaluateUnary(e)(truncateTime(DateTimeFieldType.year()))
      case TruncMonthExpr(e)  => evaluateUnary(e)(truncateTime(DateTimeFieldType.monthOfYear()))
      case TruncDayExpr(e)    => evaluateUnary(e)(truncateTime(DateTimeFieldType.dayOfMonth()))
      case TruncWeekExpr(e)   => evaluateUnary(e)(truncateTime(DateTimeFieldType.weekOfWeekyear()))
      case TruncHourExpr(e)   => evaluateUnary(e)(truncateTime(DateTimeFieldType.hourOfDay()))
      case TruncMinuteExpr(e) => evaluateUnary(e)(truncateTime(DateTimeFieldType.minuteOfHour()))
      case TruncSecondExpr(e) => evaluateUnary(e)(truncateTime(DateTimeFieldType.secondOfMinute()))

      case ExtractYearExpr(e)   => evaluateUnary(e)(_.toLocalDateTime.getYear)
      case ExtractMonthExpr(e)  => evaluateUnary(e)(_.toLocalDateTime.getMonthOfYear)
      case ExtractDayExpr(e)    => evaluateUnary(e)(_.toLocalDateTime.getDayOfMonth)
      case ExtractHourExpr(e)   => evaluateUnary(e)(_.toLocalDateTime.getHourOfDay)
      case ExtractMinuteExpr(e) => evaluateUnary(e)(_.toLocalDateTime.getMinuteOfHour)
      case ExtractSecondExpr(e) => evaluateUnary(e)(_.toLocalDateTime.getSecondOfMinute)

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

      case IsNullExpr(e)    => eval(e) == null
      case IsNotNullExpr(e) => eval(e) != null

      case TypeConvertExpr(tc, e) => tc.convert(eval(e))

      case InExpr(e, vs) => vs contains eval(e)

      case NotInExpr(e, vs) =>
        val eValue = eval(e)
        eValue != null && !vs.contains(eValue)

      case AndExpr(cs) =>
        val executed = cs.map(c => eval(c))
        executed.reduce((a, b) => a && b)

      case OrExpr(cs) =>
        val executed = cs.map(c => eval(c))
        executed.reduce((a, b) => a || b)

      case TupleExpr(e1, e2) => (eval(e1), eval(e2))

      case LowerExpr(e)  => evaluateUnary(e)(_.toLowerCase)
      case UpperExpr(e)  => evaluateUnary(e)(_.toUpperCase)
      case LengthExpr(e) => evaluateUnary(e)(_.length)
      case SplitExpr(e)  => evaluateUnary(e)(s => ExpressionCalculator.splitBy(s, !_.isLetterOrDigit).toSeq)
      case TokensExpr(e) => evaluateUnary(e)(s => tokenizer.transliteratedTokens(s))

      case a @ AbsExpr(e)        => evaluateUnary(e)(a.num.abs)
      case u @ UnaryMinusExpr(e) => evaluateUnary(e)(u.num.negate)

      case NotExpr(e) => evaluateUnary(e)(x => !x)

      case TimeMinusExpr(a, b)        => evaluateBinary(a, b)((x, y) => math.abs(x.millis - y.millis))
      case TimeMinusPeriodExpr(a, b)  => evaluateBinary(a, b)((t, p) => Time(t.toDateTime.minus(p).getMillis))
      case TimePlusPeriodExpr(a, b)   => evaluateBinary(a, b)((t, p) => Time(t.toDateTime.plus(p).getMillis))
      case PeriodPlusPeriodExpr(a, b) => evaluateBinary(a, b)((x, y) => x plus y)

      case ArrayTokensExpr(e)   => evaluateUnary(e)(a => a.flatMap(s => tokenizer.transliteratedTokens(s)))
      case ArrayLengthExpr(e)   => evaluateUnary(e)(_.length)
      case ArrayToStringExpr(e) => evaluateUnary(e)(_.mkString(", "))

      case ContainsExpr(a, b)     => evaluateBinary(a, b)(_ contains _)
      case ContainsAllExpr(a, b)  => evaluateBinary(a, b)((x, y) => y.forall(x.contains))
      case ContainsAnyExpr(a, b)  => evaluateBinary(a, b)((x, y) => y.exists(x.contains))
      case ContainsSameExpr(a, b) => evaluateBinary(a, b)((x, y) => x.size == y.size && x.toSet == y.toSet)

      case ArrayExpr(es) => es.map(e => eval(e))
    }
  }

  private def evaluateUnary[A, O](e: Expression[A])(f: A => O): O = {
    val ev = eval(e)
    if (ev != null) f(ev) else null.asInstanceOf[O]
  }

  private def evaluateBinary[A, B, O](a: Expression[A], b: Expression[B])(f: (A, B) => O): O = {
    val left = eval(a)
    val right = eval(b)
    if (left != null && right != null) {
      f(left, right)
    } else {
      null.asInstanceOf[O]
    }
  }
}
