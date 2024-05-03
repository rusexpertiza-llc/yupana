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
  trait Transformer[M[_]] {
    def apply[T](e: Expression[T]): Option[M[Expression[T]]]
  }

  trait Monad[M[_]] {
    def map[A, B](x: M[A])(f: A => B): M[B]
    def flatMap[A, B](x: M[A])(f: A => M[B]): M[B]
    def pure[A](x: A): M[A]
    def traverseSeq[A, B](xs: Seq[A])(f: A => M[B]): M[Seq[B]]
  }

  type Id[T] = T
  type TransformError[T] = Either[String, T]

  implicit val idMonad: Monad[Id] = new Monad[Id] {
    override def map[A, B](x: Id[A])(f: A => B): Id[B] = f(x)
    override def flatMap[A, B](x: Id[A])(f: A => Id[B]): Id[B] = f(x)
    override def pure[A](x: A): Id[A] = x
    override def traverseSeq[A, B](xs: Seq[A])(f: A => Id[B]): Id[Seq[B]] = xs.map(f)
  }

  implicit val teMonad: Monad[TransformError] = new Monad[TransformError] {
    override def map[A, B](x: TransformError[A])(f: A => B): TransformError[B] = x.map(f)
    override def flatMap[A, B](x: TransformError[A])(f: A => TransformError[B]): TransformError[B] = x.flatMap(f)
    override def pure[A](x: A): TransformError[A] = Right(x)
    override def traverseSeq[A, B](xs: Seq[A])(f: A => TransformError[B]): TransformError[Seq[B]] = {
      xs.foldRight(Right(List.empty[B]).withLeft[String])((v, a) => a.flatMap(bs => f(v).map(b => b :: bs)))
    }
  }

  def transform[T, M[_]](t: Transformer[M])(expr: Expression[T])(implicit m: Monad[M]): M[Expression[T]] = {
    t(expr).getOrElse(expr match {
      case TimeExpr                  => m.pure(expr)
      case NullExpr(_)               => m.pure(expr)
      case ConstantExpr(_, _)        => m.pure(expr)
      case PlaceholderExpr(_, _)     => m.pure(expr)
      case UntypedPlaceholderExpr(_) => m.pure(expr)
      case TrueExpr                  => m.pure(expr)
      case FalseExpr                 => m.pure(expr)
      case LinkExpr(_, _)            => m.pure(expr)
      case MetricExpr(_)             => m.pure(expr)
      case DimensionExpr(_)          => m.pure(expr)
      case DimensionIdExpr(_)        => m.pure(expr)
      case DimIdInExpr(_, _)         => m.pure(expr)
      case DimIdNotInExpr(_, _)      => m.pure(expr)
      case NowExpr                   => m.pure(expr)

      case EqExpr(a, b)  => m.flatMap(transform(t)(a))(ta => m.map(transform(t)(b))(tb => EqExpr(ta, tb)))
      case NeqExpr(a, b) => m.flatMap(transform(t)(a))(ta => m.map(transform(t)(b))(tb => NeqExpr(ta, tb)))
      case e @ LtExpr(a, b) =>
        m.flatMap(transform(t)(a))(ta => m.map(transform(t)(b))(tb => LtExpr(ta, tb)(e.ordering)))
      case e @ GtExpr(a, b) =>
        m.flatMap(transform(t)(a))(ta => m.map(transform(t)(b))(tb => GtExpr(ta, tb)(e.ordering)))
      case e @ LeExpr(a, b) =>
        m.flatMap(transform(t)(a))(ta => m.map(transform(t)(b))(tb => LeExpr(ta, tb)(e.ordering)))
      case e @ GeExpr(a, b) =>
        m.flatMap(transform(t)(a))(ta => m.map(transform(t)(b))(tb => GeExpr(ta, tb)(e.ordering)))
      case InExpr(e, vs)    => m.map(transform(t)(e))(te => InExpr(te, vs))
      case NotInExpr(e, vs) => m.map(transform(t)(e))(te => NotInExpr(te, vs))
      case IsNullExpr(e)    => m.map(transform(t)(e))(IsNullExpr.apply)
      case IsNotNullExpr(e) => m.map(transform(t)(e))(IsNotNullExpr.apply)
      case AndExpr(es)      => m.map(m.traverseSeq(es)(transform(t)))(AndExpr.apply)
      case OrExpr(es)       => m.map(m.traverseSeq(es)(transform(t)))(OrExpr.apply)
      case NotExpr(e)       => m.map(transform(t)(e))(NotExpr.apply)

      case e @ PlusExpr(a, b) =>
        m.flatMap(transform(t)(a))(ta => m.map(transform(t)(b))(tb => PlusExpr(ta, tb)(e.numeric)))
      case e @ MinusExpr(a, b) =>
        m.flatMap(transform(t)(a))(ta => m.map(transform(t)(b))(tb => MinusExpr(ta, tb)(e.numeric)))
      case e @ TimesExpr(a, b) =>
        m.flatMap(transform(t)(a))(ta => m.map(transform(t)(b))(tb => TimesExpr(ta, tb)(e.numeric)))
      case e @ DivIntExpr(a, b) =>
        m.flatMap(transform(t)(a))(ta => m.map(transform(t)(b))(tb => DivIntExpr(ta, tb)(e.integral)))
      case e @ DivFracExpr(a, b) =>
        m.flatMap(transform(t)(a))(ta => m.map(transform(t)(b))(tb => DivFracExpr(ta, tb)(e.fractional)))
      case e @ UnaryMinusExpr(a) => m.map(transform(t)(a))(ta => UnaryMinusExpr(ta)(e.num))
      case e @ AbsExpr(a)        => m.map(transform(t)(a))(ta => AbsExpr(ta)(e.num))

      case Double2BigDecimalExpr(e) => m.map(transform(t)(e))(Double2BigDecimalExpr.apply)
      case BigDecimal2DoubleExpr(e) => m.map(transform(t)(e))(BigDecimal2DoubleExpr.apply)
      case Long2DoubleExpr(e)       => m.map(transform(t)(e))(Long2DoubleExpr.apply)
      case Long2BigDecimalExpr(e)   => m.map(transform(t)(e))(Long2BigDecimalExpr.apply)
      case Int2LongExpr(e)          => m.map(transform(t)(e))(Int2LongExpr.apply)
      case Int2DoubleExpr(e)        => m.map(transform(t)(e))(Int2DoubleExpr.apply)
      case Int2BigDecimalExpr(e)    => m.map(transform(t)(e))(Int2BigDecimalExpr.apply)
      case Short2IntExpr(e)         => m.map(transform(t)(e))(Short2IntExpr.apply)
      case Short2LongExpr(e)        => m.map(transform(t)(e))(Short2LongExpr.apply)
      case Short2DoubleExpr(e)      => m.map(transform(t)(e))(Short2DoubleExpr.apply)
      case Short2BigDecimalExpr(e)  => m.map(transform(t)(e))(Short2BigDecimalExpr.apply)
      case Byte2ShortExpr(e)        => m.map(transform(t)(e))(Byte2ShortExpr.apply)
      case Byte2IntExpr(e)          => m.map(transform(t)(e))(Byte2IntExpr.apply)
      case Byte2LongExpr(e)         => m.map(transform(t)(e))(Byte2LongExpr.apply)
      case Byte2DoubleExpr(e)       => m.map(transform(t)(e))(Byte2DoubleExpr.apply)
      case Byte2BigDecimalExpr(e)   => m.map(transform(t)(e))(Byte2BigDecimalExpr.apply)
      case ToStringExpr(e)          => m.map(transform(t)(e))(te => ToStringExpr(te)(e.dataType))

      case TupleExpr(a, b) =>
        m.flatMap(transform(t)(a))(ta => m.map(transform(t)(b))(tb => TupleExpr(ta, tb)(a.dataType, b.dataType)))
      case ae @ ArrayExpr(es) => m.map(m.traverseSeq(es)(transform(t)))(xs => ArrayExpr(xs)(ae.elementDataType))

      case ContainsExpr(a, b) => m.flatMap(transform(t)(a))(ta => m.map(transform(t)(b))(tb => ContainsExpr(ta, tb)))
      case ContainsAnyExpr(a, b) =>
        m.flatMap(transform(t)(a))(ta => m.map(transform(t)(b))(tb => ContainsAnyExpr(ta, tb)))
      case ContainsAllExpr(a, b) =>
        m.flatMap(transform(t)(a))(ta => m.map(transform(t)(b))(tb => ContainsAllExpr(ta, tb)))
      case ContainsSameExpr(a, b) =>
        m.flatMap(transform(t)(a))(ta => m.map(transform(t)(b))(tb => ContainsSameExpr(ta, tb)))
      case ArrayLengthExpr(a)   => m.map(transform(t)(a))(ArrayLengthExpr.apply)
      case ArrayToStringExpr(a) => m.map(transform(t)(a))(ArrayToStringExpr.apply)
      case ArrayTokensExpr(a)   => m.map(transform(t)(a))(ArrayTokensExpr.apply)

      case UpperExpr(e)     => m.map(transform(t)(e))(UpperExpr.apply)
      case LowerExpr(e)     => m.map(transform(t)(e))(LowerExpr.apply)
      case ConcatExpr(a, b) => m.flatMap(transform(t)(a))(ta => m.map(transform(t)(b))(tb => ConcatExpr(ta, tb)))
      case SplitExpr(e)     => m.map(transform(t)(e))(SplitExpr.apply)
      case TokensExpr(e)    => m.map(transform(t)(e))(TokensExpr.apply)
      case LengthExpr(e)    => m.map(transform(t)(e))(LengthExpr.apply)

      case ExtractYearExpr(e)    => m.map(transform(t)(e))(ExtractYearExpr.apply)
      case ExtractQuarterExpr(e) => m.map(transform(t)(e))(ExtractQuarterExpr.apply)
      case ExtractMonthExpr(e)   => m.map(transform(t)(e))(ExtractMonthExpr.apply)
      case ExtractDayExpr(e)     => m.map(transform(t)(e))(ExtractDayExpr.apply)
      case ExtractHourExpr(e)    => m.map(transform(t)(e))(ExtractHourExpr.apply)
      case ExtractMinuteExpr(e)  => m.map(transform(t)(e))(ExtractMinuteExpr.apply)
      case ExtractSecondExpr(e)  => m.map(transform(t)(e))(ExtractSecondExpr.apply)

      case TruncYearExpr(e)    => m.map(transform(t)(e))(TruncYearExpr.apply)
      case TruncQuarterExpr(e) => m.map(transform(t)(e))(TruncQuarterExpr.apply)
      case TruncMonthExpr(e)   => m.map(transform(t)(e))(TruncMonthExpr.apply)
      case TruncWeekExpr(e)    => m.map(transform(t)(e))(TruncWeekExpr.apply)
      case TruncDayExpr(e)     => m.map(transform(t)(e))(TruncDayExpr.apply)
      case TruncHourExpr(e)    => m.map(transform(t)(e))(TruncHourExpr.apply)
      case TruncMinuteExpr(e)  => m.map(transform(t)(e))(TruncMinuteExpr.apply)
      case TruncSecondExpr(e)  => m.map(transform(t)(e))(TruncSecondExpr.apply)

      case PeriodPlusPeriodExpr(a, b) =>
        m.flatMap(transform(t)(a))(ta => m.map(transform(t)(b))(tb => PeriodPlusPeriodExpr(ta, tb)))
      case TimeMinusExpr(a, b) => m.flatMap(transform(t)(a))(ta => m.map(transform(t)(b))(tb => TimeMinusExpr(ta, tb)))
      case TimeMinusPeriodExpr(a, b) =>
        m.flatMap(transform(t)(a))(ta => m.map(transform(t)(b))(tb => TimeMinusPeriodExpr(ta, tb)))
      case TimePlusPeriodExpr(a, b) =>
        m.flatMap(transform(t)(a))(ta => m.map(transform(t)(b))(tb => TimePlusPeriodExpr(ta, tb)))

      case ConditionExpr(c, p, n) =>
        m.flatMap(transform(t)(c))(tc =>
          m.flatMap(transform(t)(p))(tp => m.map(transform(t)(n))(tn => ConditionExpr(tc, tp, tn)))
        )

      case s @ SumExpr(e)            => m.map(transform(t)(e))(te => SumExpr(te)(s.numeric, s.dt, s.guard))
      case me @ MaxExpr(e)           => m.map(transform(t)(e))(te => MaxExpr(te)(me.ord))
      case me @ MinExpr(e)           => m.map(transform(t)(e))(te => MinExpr(te)(me.ord))
      case s @ AvgExpr(e)            => m.map(transform(t)(e))(te => AvgExpr(te)(s.numeric))
      case CountExpr(e)              => m.map(transform(t)(e))(CountExpr.apply)
      case DistinctCountExpr(e)      => m.map(transform(t)(e))(DistinctCountExpr.apply)
      case HLLCountExpr(e, accuracy) => m.map(transform(t)(e))(te => HLLCountExpr(te, accuracy))
      case DistinctRandomExpr(e)     => m.map(transform(t)(e))(DistinctRandomExpr.apply)

      case LagExpr(e) => m.map(transform(t)(e))(LagExpr.apply)

      // !!! DO NOT ADD DEFAULT CASE HERE, to keep matching exhaustive !!!
    })
  }
}
