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

package org.yupana.core.utils

import org.yupana.api.query._

object ExpressionUtils {
  trait Transformer {
    def apply[T](e: Expression[T]): Option[Expression[T]]
    def isDefinedAt[T](e: Expression[T]): Boolean = apply(e).nonEmpty
  }

  def transform[T](t: Transformer)(expr: Expression[T]): Expression[T] = {
    t(expr).getOrElse(expr match {
      case TimeExpr             => expr
      case ConstantExpr(_, _)   => expr
      case LinkExpr(_, _)       => expr
      case MetricExpr(_)        => expr
      case DimensionExpr(_)     => expr
      case DimensionIdExpr(_)   => expr
      case DimIdInExpr(_, _)    => expr
      case DimIdNotInExpr(_, _) => expr

      case EqExpr(a, b)     => EqExpr(transform(t)(a), transform(t)(b))
      case NeqExpr(a, b)    => NeqExpr(transform(t)(a), transform(t)(b))
      case e @ LtExpr(a, b) => LtExpr(transform(t)(a), transform(t)(b))(e.ordering)
      case e @ GtExpr(a, b) => GtExpr(transform(t)(a), transform(t)(b))(e.ordering)
      case e @ LeExpr(a, b) => LeExpr(transform(t)(a), transform(t)(b))(e.ordering)
      case e @ GeExpr(a, b) => GeExpr(transform(t)(a), transform(t)(b))(e.ordering)
      case InExpr(e, vs)    => InExpr(transform(t)(e), vs)
      case NotInExpr(e, vs) => NotInExpr(transform(t)(e), vs)
      case IsNullExpr(e)    => IsNullExpr(e)
      case IsNotNullExpr(e) => IsNotNullExpr(e)
      case AndExpr(es)      => AndExpr(es.map(transform(t)))
      case OrExpr(es)       => OrExpr(es.map(transform(t)))
      case NotExpr(e)       => NotExpr(transform(t)(e))

      case e @ PlusExpr(a, b)    => PlusExpr(transform(t)(a), transform(t)(b))(e.numeric)
      case e @ MinusExpr(a, b)   => MinusExpr(transform(t)(a), transform(t)(b))(e.numeric)
      case e @ TimesExpr(a, b)   => TimesExpr(transform(t)(a), transform(t)(b))(e.numeric)
      case e @ DivIntExpr(a, b)  => DivIntExpr(transform(t)(a), transform(t)(b))(e.integral)
      case e @ DivFracExpr(a, b) => DivFracExpr(transform(t)(a), transform(t)(b))(e.fractional)
      case e @ UnaryMinusExpr(a) => UnaryMinusExpr(transform(t)(a))(e.num)
      case e @ AbsExpr(a)        => AbsExpr(transform(t)(a))(e.num)

      case TypeConvertExpr(c, e) => TypeConvertExpr(c, transform(t)(e))

      case TupleExpr(a, b)    => TupleExpr(transform(t)(a), transform(t)(b))(a.dataType, b.dataType)
      case ae @ ArrayExpr(es) => ArrayExpr(es.map(transform(t)))(ae.elementDataType)

      case ContainsExpr(a, b)     => ContainsExpr(transform(t)(a), transform(t)(b))
      case ContainsAnyExpr(a, b)  => ContainsAnyExpr(transform(t)(a), transform(t)(b))
      case ContainsAllExpr(a, b)  => ContainsAllExpr(transform(t)(a), transform(t)(b))
      case ContainsSameExpr(a, b) => ContainsSameExpr(transform(t)(a), transform(t)(b))
      case ArrayLengthExpr(a)     => ArrayLengthExpr(transform(t)(a))
      case ArrayToStringExpr(a)   => ArrayToStringExpr(transform(t)(a))
      case ArrayTokensExpr(a)     => ArrayTokensExpr(transform(t)(a))

      case UpperExpr(e)     => UpperExpr(transform(t)(e))
      case LowerExpr(e)     => LowerExpr(transform(t)(e))
      case ConcatExpr(a, b) => ConcatExpr(transform(t)(a), transform(t)(b))
      case SplitExpr(e)     => SplitExpr(transform(t)(e))
      case TokensExpr(e)    => TokensExpr(transform(t)(e))
      case LengthExpr(e)    => LengthExpr(transform(t)(e))

      case ExtractYearExpr(e)   => ExtractYearExpr(transform(t)(e))
      case ExtractMonthExpr(e)  => ExtractMonthExpr(transform(t)(e))
      case ExtractDayExpr(e)    => ExtractDayExpr(transform(t)(e))
      case ExtractHourExpr(e)   => ExtractHourExpr(transform(t)(e))
      case ExtractMinuteExpr(e) => ExtractMinuteExpr(transform(t)(e))
      case ExtractSecondExpr(e) => ExtractSecondExpr(transform(t)(e))

      case TruncYearExpr(e)   => TruncYearExpr(transform(t)(e))
      case TruncMonthExpr(e)  => TruncMonthExpr(transform(t)(e))
      case TruncWeekExpr(e)   => TruncWeekExpr(transform(t)(e))
      case TruncDayExpr(e)    => TruncDayExpr(transform(t)(e))
      case TruncHourExpr(e)   => TruncHourExpr(transform(t)(e))
      case TruncMinuteExpr(e) => TruncMinuteExpr(transform(t)(e))
      case TruncSecondExpr(e) => TruncSecondExpr(transform(t)(e))

      case PeriodPlusPeriodExpr(a, b) => PeriodPlusPeriodExpr(transform(t)(a), transform(t)(b))
      case TimeMinusExpr(a, b)        => TimeMinusExpr(transform(t)(a), transform(t)(b))
      case TimeMinusPeriodExpr(a, b)  => TimeMinusPeriodExpr(transform(t)(a), transform(t)(b))
      case TimePlusPeriodExpr(a, b)   => TimePlusPeriodExpr(transform(t)(a), transform(t)(b))

      case ConditionExpr(c, p, n) => ConditionExpr(transform(t)(c), transform(t)(p), transform(t)(n))

      case s @ SumExpr(e)        => SumExpr(transform(t)(e))(s.numeric)
      case m @ MaxExpr(e)        => MaxExpr(transform(t)(e))(m.ord)
      case m @ MinExpr(e)        => MinExpr(transform(t)(e))(m.ord)
      case CountExpr(e)          => CountExpr(transform(t)(e))
      case DistinctCountExpr(e)  => DistinctCountExpr(transform(t)(e))
      case HLLCountExpr(e)       => HLLCountExpr(transform(t)(e))
      case DistinctRandomExpr(e) => DistinctRandomExpr(transform(t)(e))

      case LagExpr(e) => LagExpr(transform(t)(e))

      // !!! DO NOT ADD DEFAULT CASE HERE, to keep matching exhaustive !!!
    })
  }

}
