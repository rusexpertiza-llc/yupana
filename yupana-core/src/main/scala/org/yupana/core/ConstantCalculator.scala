package org.yupana.core

import org.joda.time.DateTimeFieldType
import org.yupana.api.Time
import org.yupana.api.query._
import org.yupana.api.utils.Tokenizer

import scala.collection.AbstractIterator

class ConstantCalculator(tokenizer: Tokenizer) extends Serializable {

  def evaluateConstant[T](expr: Expression[T]): T = {
    assert(expr.kind == Const)
    eval(expr)
  }

//  override def evaluateMap[I, M](expr: AggregateExpr[I, M, _], queryContext: QueryContext, row: InternalRow): M = {
//    val res = expr match {
//      case MinExpr(e) => row.get(queryContext, e)
//      case MaxExpr(e) => row.get(queryContext, e)
//      case SumExpr(e) => row.get(queryContext, e)
//      case CountExpr(e) =>
//        val v = row.get(queryContext, e)
//        if (v != null) 1L else 0L
//      case DistinctCountExpr(e) =>
//        val v = row.get(queryContext, e)
//        if (v != null) Set(v) else Set.empty[I]
//      case DistinctRandomExpr(e) =>
//        val v = row.get(queryContext, e)
//        if (v != null) Set(v) else Set.empty[I]
//    }
//
//    res.asInstanceOf[M]
//  }
//
//  override def evaluateReduce[M](
//      expr: AggregateExpr[_, M, _],
//      queryContext: QueryContext,
//      a: InternalRow,
//      b: InternalRow
//  ): M = {
//    def reduce(x: M, y: M): M = {
//      val res = expr match {
//        case m: MinExpr[_]         => m.ord.min(x, y)
//        case m: MaxExpr[_]         => m.ord.max(x, y)
//        case s: SumExpr[_]         => s.numeric.plus(x, y)
//        case CountExpr(_)          => x.asInstanceOf[Long] + y.asInstanceOf[Long]
//        case DistinctCountExpr(_)  => x.asInstanceOf[Set[_]] ++ y.asInstanceOf[Set[_]]
//        case DistinctRandomExpr(_) => x.asInstanceOf[Set[_]] ++ y.asInstanceOf[Set[_]]
//      }
//
//      res.asInstanceOf[M]
//    }
//
//    val aValue = a.get(queryContext, expr).asInstanceOf[M]
//    val bValue = b.get(queryContext, expr).asInstanceOf[M]
//
//    if (aValue != null) {
//      if (bValue != null) {
//        reduce(aValue, bValue)
//      } else aValue
//    } else bValue
//  }
//
//  override def evaluatePostMap[M, O](expr: AggregateExpr[_, M, O], queryContext: QueryContext, row: InternalRow): O = {
//    val oldValue = row.get(queryContext, expr)
//
//    val res = expr match {
//      case MinExpr(_)           => oldValue
//      case MaxExpr(_)           => oldValue
//      case s @ SumExpr(_)       => if (oldValue != null) oldValue else s.numeric.zero
//      case CountExpr(_)         => oldValue
//      case DistinctCountExpr(_) => oldValue.asInstanceOf[Set[_]].size
//      case DistinctRandomExpr(_) =>
//        val s = oldValue.asInstanceOf[Set[_]]
//        val n = util.Random.nextInt(s.size)
//        s.iterator.drop(n).next
//    }
//
//    res.asInstanceOf[O]
//  }

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
      case SplitExpr(e)  => evaluateUnary(e)(s => splitBy(s, !_.isLetterOrDigit).toSeq)
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
