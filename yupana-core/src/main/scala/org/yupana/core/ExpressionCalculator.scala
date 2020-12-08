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
import org.yupana.core.model.InternalRow

import scala.collection.AbstractIterator

class ExpressionCalculator(tokenizer: Tokenizer) extends Serializable {

  def evaluateConstant[T](expr: Expression[T]): T = {
    assert(expr.kind == Const)
    eval(expr, null, null)
  }

  def preEvaluated[T](expr: Expression[T], queryContext: QueryContext, internalRow: InternalRow): T = {
    expr match {
      case ConstantExpr(v) => v
      case _               => internalRow.get[T](queryContext, expr)
    }
  }

  def evaluateExpression[T](
      expr: Expression[T],
      queryContext: QueryContext,
      internalRow: InternalRow
  ): T = {
    val res = if (queryContext != null) {
      val idx = queryContext.exprsIndex.getOrElse(expr, -1)
      if (idx >= 0) {
        internalRow.get[T](idx)
      } else {
        null.asInstanceOf[T]
      }
    } else {
      null.asInstanceOf[T]
    }

    if (res == null) {
      eval(expr, queryContext, internalRow)
    } else {
      res
    }
  }

  def evaluateMap[I, M](expr: AggregateExpr[I, M, _], queryContext: QueryContext, row: InternalRow): M = {
    val res = expr match {
      case MinExpr(e) => row.get(queryContext, e)
      case MaxExpr(e) => row.get(queryContext, e)
      case SumExpr(e) => row.get(queryContext, e)
      case CountExpr(e) =>
        val v = row.get(queryContext, e)
        if (v != null) 1L else 0L
      case DistinctCountExpr(e) =>
        val v = row.get(queryContext, e)
        if (v != null) Set(v) else Set.empty[I]
      case DistinctRandomExpr(e) =>
        val v = row.get(queryContext, e)
        if (v != null) Set(v) else Set.empty[I]
    }

    res.asInstanceOf[M]
  }

  def evaluateReduce[M](
      expr: AggregateExpr[_, M, _],
      queryContext: QueryContext,
      a: InternalRow,
      b: InternalRow
  ): M = {
    def reduce(x: M, y: M): M = {
      val res = expr match {
        case m: MinExpr[_]         => m.ord.min(x, y)
        case m: MaxExpr[_]         => m.ord.max(x, y)
        case s: SumExpr[_]         => s.numeric.plus(x, y)
        case CountExpr(_)          => x.asInstanceOf[Long] + y.asInstanceOf[Long]
        case DistinctCountExpr(_)  => x.asInstanceOf[Set[_]] ++ y.asInstanceOf[Set[_]]
        case DistinctRandomExpr(_) => x.asInstanceOf[Set[_]] ++ y.asInstanceOf[Set[_]]
      }

      res.asInstanceOf[M]
    }

    val aValue = a.get(queryContext, expr).asInstanceOf[M]
    val bValue = b.get(queryContext, expr).asInstanceOf[M]

    if (aValue != null) {
      if (bValue != null) {
        reduce(aValue, bValue)
      } else aValue
    } else bValue
  }

  def evaluatePostMap[M, O](expr: AggregateExpr[_, M, O], queryContext: QueryContext, row: InternalRow): O = {
    val oldValue = row.get(queryContext, expr)

    val res = expr match {
      case MinExpr(_)           => oldValue
      case MaxExpr(_)           => oldValue
      case s @ SumExpr(_)       => if (oldValue != null) oldValue else s.numeric.zero
      case CountExpr(_)         => oldValue
      case DistinctCountExpr(_) => oldValue.asInstanceOf[Set[_]].size
      case DistinctRandomExpr(_) =>
        val s = oldValue.asInstanceOf[Set[_]]
        val n = util.Random.nextInt(s.size)
        s.iterator.drop(n).next
    }

    res.asInstanceOf[O]
  }

  private def eval[T](expr: Expression[T], qc: QueryContext, row: InternalRow): T = {

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
      case ae: AggregateExpr[_, _, _]   => evaluateExpression(ae.expr, qc, row).asInstanceOf[T]
      case we: WindowFunctionExpr[_, _] => evaluateExpression(we.expr, qc, row).asInstanceOf[T]

      case ConditionExpr(condition, positive, negative) =>
        val x = evaluateExpression(condition, qc, row)
        if (x) {
          evaluateExpression(positive, qc, row)
        } else {
          evaluateExpression(negative, qc, row)
        }

      case TruncYearExpr(e)   => evaluateUnary(qc, row, e)(truncateTime(DateTimeFieldType.year()))
      case TruncMonthExpr(e)  => evaluateUnary(qc, row, e)(truncateTime(DateTimeFieldType.monthOfYear()))
      case TruncDayExpr(e)    => evaluateUnary(qc, row, e)(truncateTime(DateTimeFieldType.dayOfMonth()))
      case TruncWeekExpr(e)   => evaluateUnary(qc, row, e)(truncateTime(DateTimeFieldType.weekOfWeekyear()))
      case TruncHourExpr(e)   => evaluateUnary(qc, row, e)(truncateTime(DateTimeFieldType.hourOfDay()))
      case TruncMinuteExpr(e) => evaluateUnary(qc, row, e)(truncateTime(DateTimeFieldType.minuteOfHour()))
      case TruncSecondExpr(e) => evaluateUnary(qc, row, e)(truncateTime(DateTimeFieldType.secondOfMinute()))

      case ExtractYearExpr(e)   => evaluateUnary(qc, row, e)(_.toLocalDateTime.getYear)
      case ExtractMonthExpr(e)  => evaluateUnary(qc, row, e)(_.toLocalDateTime.getMonthOfYear)
      case ExtractDayExpr(e)    => evaluateUnary(qc, row, e)(_.toLocalDateTime.getDayOfMonth)
      case ExtractHourExpr(e)   => evaluateUnary(qc, row, e)(_.toLocalDateTime.getHourOfDay)
      case ExtractMinuteExpr(e) => evaluateUnary(qc, row, e)(_.toLocalDateTime.getMinuteOfHour)
      case ExtractSecondExpr(e) => evaluateUnary(qc, row, e)(_.toLocalDateTime.getSecondOfMinute)

      case p @ PlusExpr(a, b)    => evaluateBinary(qc, row, a, b)(p.numeric.plus)
      case m @ MinusExpr(a, b)   => evaluateBinary(qc, row, a, b)(m.numeric.minus)
      case t @ TimesExpr(a, b)   => evaluateBinary(qc, row, a, b)(t.numeric.times)
      case d @ DivIntExpr(a, b)  => evaluateBinary(qc, row, a, b)(d.integral.quot)
      case d @ DivFracExpr(a, b) => evaluateBinary(qc, row, a, b)(d.fractional.div)

      case ConcatExpr(a, b) => evaluateBinary(qc, row, a, b)(_ ++ _)

      case EqExpr(a, b)     => evaluateBinary(qc, row, a, b)(_ == _)
      case NeqExpr(a, b)    => evaluateBinary(qc, row, a, b)(_ != _)
      case e @ GtExpr(a, b) => evaluateBinary(qc, row, a, b)(e.ordering.gt)
      case e @ LtExpr(a, b) => evaluateBinary(qc, row, a, b)(e.ordering.lt)
      case e @ GeExpr(a, b) => evaluateBinary(qc, row, a, b)(e.ordering.gteq)
      case e @ LeExpr(a, b) => evaluateBinary(qc, row, a, b)(e.ordering.lteq)

      case IsNullExpr(e)    => evaluateExpression(e, qc, row) == null
      case IsNotNullExpr(e) => evaluateExpression(e, qc, row) != null

      case TypeConvertExpr(tc, e) =>
        tc.convert(evaluateExpression(e, qc, row))

      case InExpr(e, vs) =>
        vs contains evaluateExpression(e, qc, row)

      case NotInExpr(e, vs) =>
        val eValue = evaluateExpression(e, qc, row)
        eValue != null && !vs.contains(eValue)

      case AndExpr(cs) =>
        val executed = cs.map(c => evaluateExpression(c, qc, row))
        executed.reduce((a, b) => a && b)

      case OrExpr(cs) =>
        val executed = cs.map(c => evaluateExpression(c, qc, row))
        executed.reduce((a, b) => a || b)

      case TupleExpr(e1, e2) =>
        (evaluateExpression(e1, qc, row), evaluateExpression(e2, qc, row))

      case LowerExpr(e)  => evaluateUnary(qc, row, e)(_.toLowerCase)
      case UpperExpr(e)  => evaluateUnary(qc, row, e)(_.toUpperCase)
      case LengthExpr(e) => evaluateUnary(qc, row, e)(_.length)
      case SplitExpr(e)  => evaluateUnary(qc, row, e)(s => splitBy(s, !_.isLetterOrDigit).toSeq)
      case TokensExpr(e) => evaluateUnary(qc, row, e)(s => tokenizer.transliteratedTokens(s))

      case a @ AbsExpr(e)        => evaluateUnary(qc, row, e)(a.num.abs)
      case u @ UnaryMinusExpr(e) => evaluateUnary(qc, row, e)(u.num.negate)

      case NotExpr(e) => evaluateUnary(qc, row, e)(x => !x)

      case TimeMinusExpr(a, b)        => evaluateBinary(qc, row, a, b)((x, y) => math.abs(x.millis - y.millis))
      case TimeMinusPeriodExpr(a, b)  => evaluateBinary(qc, row, a, b)((t, p) => Time(t.toDateTime.minus(p).getMillis))
      case TimePlusPeriodExpr(a, b)   => evaluateBinary(qc, row, a, b)((t, p) => Time(t.toDateTime.plus(p).getMillis))
      case PeriodPlusPeriodExpr(a, b) => evaluateBinary(qc, row, a, b)((x, y) => x plus y)

      case ArrayTokensExpr(e)   => evaluateUnary(qc, row, e)(a => a.flatMap(s => tokenizer.transliteratedTokens(s)))
      case ArrayLengthExpr(e)   => evaluateUnary(qc, row, e)(_.length)
      case ArrayToStringExpr(e) => evaluateUnary(qc, row, e)(_.mkString(", "))

      case ContainsExpr(a, b)     => evaluateBinary(qc, row, a, b)(_ contains _)
      case ContainsAllExpr(a, b)  => evaluateBinary(qc, row, a, b)((x, y) => y.forall(x.contains))
      case ContainsAnyExpr(a, b)  => evaluateBinary(qc, row, a, b)((x, y) => y.exists(x.contains))
      case ContainsSameExpr(a, b) => evaluateBinary(qc, row, a, b)(_ == _)

      case ArrayExpr(es) => es.map(e => evaluateExpression(e, qc, row))
    }
  }

  def evaluateWindow[I, O](winFuncExpr: WindowFunctionExpr[I, O], values: Array[I], index: Int): O = {
    winFuncExpr match {
      case LagExpr(_) => if (index > 0) values(index - 1).asInstanceOf[O] else null.asInstanceOf[O]
    }
  }

  private def evaluateUnary[A, O](qc: QueryContext, internalRow: InternalRow, e: Expression[A])(f: A => O): O = {
    val ev = evaluateExpression(e, qc, internalRow)
    if (ev != null) f(ev) else null.asInstanceOf[O]
  }

  private def evaluateBinary[A, B, O](qc: QueryContext, internalRow: InternalRow, a: Expression[A], b: Expression[B])(
      f: (A, B) => O
  ): O = {
    val left = evaluateExpression(a, qc, internalRow)
    val right = evaluateExpression(b, qc, internalRow)
    if (left != null && right != null) {
      f(left, right)
    } else {
      null.asInstanceOf[O]
    }
  }

  private def truncateTime(fieldType: DateTimeFieldType)(time: Time): Time = {
    Time(time.toDateTime.property(fieldType).roundFloorCopy().getMillis)
  }

  private def splitBy(s: String, p: Char => Boolean): Iterator[String] = new AbstractIterator[String] {
    private val len = s.length
    private var pos = 0

    override def hasNext: Boolean = pos < len

    override def next(): String = {
      if (pos >= len) throw new NoSuchElementException("next on empty iterator")
      val start = pos
      while (pos < len && !p(s(pos))) pos += 1
      val res = s.substring(start, pos min len)
      while (pos < len && p(s(pos))) pos += 1
      res
    }
  }
}
