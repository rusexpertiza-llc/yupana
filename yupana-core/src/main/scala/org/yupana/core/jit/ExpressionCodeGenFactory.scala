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

package org.yupana.core.jit

import org.yupana.api.query._
import org.yupana.core.jit.codegen.expressions._
import org.yupana.core.jit.codegen.expressions.aggregate._
import org.yupana.core.jit.codegen.expressions.regular.{
  ArrayExpressionCodeGen,
  BinaryExpressionCodeGen,
  ConditionExpressionCodeGen,
  ConditionWithRefCodeGen,
  ConstantExpressionCodeGen,
  DivFracExpressionCodeGen,
  FieldExpressionGen,
  LogicalExpressionCodeGen,
  MathUnaryExpressionCodeGen,
  NullExpressionCodeGen,
  OrdExpressionCodeGen,
  TupleExpressionCodeGen,
  UnaryExpressionCodeGen
}

import scala.reflect.runtime.universe._

object ExpressionCodeGenFactory {

  private val calculator = q"_root_.org.yupana.core.jit.ExpressionCalculator"
  val tokenizer = TermName("tokenizer")
  private val truncTime = q"_root_.org.yupana.core.jit.ExpressionCalculator.truncateTime"
  private val truncTimeBy = q"_root_.org.yupana.core.jit.ExpressionCalculator.truncateTimeBy"
  private val monday = q"_root_.java.time.DayOfWeek.MONDAY"
  private val cru = q"_root_.java.time.temporal.ChronoUnit"
  private val adj = q"_root_.java.time.temporal.TemporalAdjusters"

  def codeGenerator(expr: Expression[_]): ExpressionCodeGen[_] = {
    new LocalDeclarationCodeGen(expr, createCodeGenerator(expr))
  }

  def aggExprCodeGenerator(e: AggregateExpr[_, _, _]): AggregateExpressionCodeGen[_] = {
    e match {
      case e: SumExpr[_, _]         => new SumExprCodeGen(e)
      case e: MinExpr[_]            => MinMaxExprCodeGen.min(e)
      case e: MaxExpr[_]            => MinMaxExprCodeGen.max(e)
      case e: CountExpr[_]          => new CountExprCodeGen(e)
      case e: AvgExpr[_]            => new AvgExprCodeGen(e)
      case e: DistinctCountExpr[_]  => DistinctExprsCodeGen.distinctCount(e)
      case e: DistinctRandomExpr[_] => DistinctExprsCodeGen.distinctRandom(e)
      case e: HLLCountExpr[_]       => new HLLCountExprCodeGen(e)
    }
  }

  def needEvaluateInProjectionStage(expr: Expression[_]): Boolean = {
    expr match {
      case ConstantExpr(c, _)   => false
      case TrueExpr             => false
      case FalseExpr            => false
      case NullExpr(_)          => false
      case TimeExpr             => false
      case DimensionExpr(_)     => false
      case DimensionIdExpr(_)   => false
      case MetricExpr(_)        => false
      case DimIdInExpr(_, _)    => false
      case DimIdNotInExpr(_, _) => false
      case LinkExpr(_, _)       => false
      case _                    => true
    }
  }

  private def createCodeGenerator(expr: Expression[_]): ExpressionCodeGen[_] = {
    expr match {
      case ConstantExpr(c, prepared) => new ConstantExpressionCodeGen(expr, c, prepared)
      case UntypedConstantExpr(s)    => throw new IllegalArgumentException(s"Untyped constant '$s' in calculator!")
      case TrueExpr                  => new ConstantExpressionCodeGen(expr, true, prepared = false)
      case FalseExpr                 => new ConstantExpressionCodeGen(expr, false, prepared = false)
      case NullExpr(_)               => new NullExpressionCodeGen(expr)
      case TimeExpr                  => new FieldExpressionGen(expr)
      case DimensionExpr(_)          => new FieldExpressionGen(expr)
      case DimensionIdExpr(_)        => new FieldExpressionGen(expr)
      case MetricExpr(_)             => new FieldExpressionGen(expr)
      case DimIdInExpr(_, _)         => new FieldExpressionGen(expr)
      case DimIdNotInExpr(_, _)      => new FieldExpressionGen(expr)
      case LinkExpr(_, _)            => new FieldExpressionGen(expr)

      case e: AggregateExpr[_, _, _] =>
        aggExprCodeGenerator(e)

      case we: WindowFunctionExpr[_, _] =>
        new WindowFunctionExprCodeGen(we)

      case e @ TupleExpr(_, _) => TupleExpressionCodeGen(e)

      case e @ GtExpr(_, _)  => OrdExpressionCodeGen(e, (x, y) => q"""$x > $y""", "gt")
      case e @ LtExpr(_, _)  => OrdExpressionCodeGen(e, (x, y) => q"""$x < $y""", "lt")
      case e @ GeExpr(_, _)  => OrdExpressionCodeGen(e, (x, y) => q"""$x >= $y""", "gteq")
      case e @ LeExpr(_, _)  => OrdExpressionCodeGen(e, (x, y) => q"""$x <= $y""", "lteq")
      case e @ EqExpr(_, _)  => BinaryExpressionCodeGen(e, (x, y) => q"""$x == $y""")
      case e @ NeqExpr(_, _) => BinaryExpressionCodeGen(e, (x, y) => q"""$x != $y""")

      case e @ InExpr(_, vs)    => ConditionWithRefCodeGen(e, vs, (v, r) => q"$r.contains($v)")
      case e @ NotInExpr(_, vs) => ConditionWithRefCodeGen(e, vs, (v, r) => q"!$r.contains($v)")

      case e @ PlusExpr(_, _)    => BinaryExpressionCodeGen(e, (x, y) => q"""$x + $y""")
      case e @ MinusExpr(_, _)   => BinaryExpressionCodeGen(e, (x, y) => q"""$x - $y""")
      case e @ TimesExpr(_, _)   => BinaryExpressionCodeGen(e, (x, y) => q"""$x * $y""")
      case e @ DivIntExpr(_, _)  => BinaryExpressionCodeGen(e, (x, y) => q"""$x / $y""")
      case e @ DivFracExpr(_, _) => DivFracExpressionCodeGen.apply(e)

      case e @ Double2BigDecimalExpr(_) => UnaryExpressionCodeGen(e, d => q"BigDecimal($d)")

      case e @ BigDecimal2DoubleExpr(_) => UnaryExpressionCodeGen(e, b => q"$b.toDouble")

      case e @ Long2BigDecimalExpr(_) => UnaryExpressionCodeGen(e, l => q"BigDecimal($l)")
      case e @ Long2DoubleExpr(_)     => UnaryExpressionCodeGen(e, l => q"$l.toDouble")

      case e @ Int2BigDecimalExpr(_) => UnaryExpressionCodeGen(e, i => q"BigDecimal($i)")
      case e @ Int2DoubleExpr(_)     => UnaryExpressionCodeGen(e, i => q"$i.toDouble")
      case e @ Int2LongExpr(_)       => UnaryExpressionCodeGen(e, i => q"$i.toLong")

      case e @ Short2BigDecimalExpr(a) => UnaryExpressionCodeGen(e, s => q"BigDecimal($s)")
      case e @ Short2DoubleExpr(a)     => UnaryExpressionCodeGen(e, s => q"$s.toDouble")
      case e @ Short2LongExpr(a)       => UnaryExpressionCodeGen(e, s => q"$s.toLong")
      case e @ Short2IntExpr(a)        => UnaryExpressionCodeGen(e, s => q"$s.toInt")

      case e @ Byte2BigDecimalExpr(a) => UnaryExpressionCodeGen(e, b => q"BigDecimal($b)")
      case e @ Byte2DoubleExpr(a)     => UnaryExpressionCodeGen(e, b => q"$b.toDouble")
      case e @ Byte2LongExpr(a)       => UnaryExpressionCodeGen(e, b => q"$b.toLong")
      case e @ Byte2IntExpr(a)        => UnaryExpressionCodeGen(e, b => q"$b.toInt")
      case e @ Byte2ShortExpr(a)      => UnaryExpressionCodeGen(e, b => q"$b.toShort")

      case e @ ToStringExpr(a) => UnaryExpressionCodeGen(e, x => q"$x.toString")

      case e @ TruncYearExpr(a) => UnaryExpressionCodeGen(e, x => q"$truncTime($adj.firstDayOfYear)($x)")
      case e @ TruncQuarterExpr(a) =>
        UnaryExpressionCodeGen(
          e,
          x =>
            q"$truncTimeBy(dTime => dTime.`with`($adj.firstDayOfMonth).withMonth(dTime.getMonth.firstMonthOfQuarter.getValue))($x)"
        )
      case e @ TruncMonthExpr(a)  => UnaryExpressionCodeGen(e, x => q"$truncTime($adj.firstDayOfMonth)($x)")
      case e @ TruncWeekExpr(a)   => UnaryExpressionCodeGen(e, x => q"$truncTime($adj.previousOrSame($monday))($x)")
      case e @ TruncDayExpr(a)    => UnaryExpressionCodeGen(e, x => q"$truncTime($cru.DAYS)($x)")
      case e @ TruncHourExpr(a)   => UnaryExpressionCodeGen(e, x => q"$truncTime($cru.HOURS)($x)")
      case e @ TruncMinuteExpr(a) => UnaryExpressionCodeGen(e, x => q"$truncTime($cru.MINUTES)($x)")
      case e @ TruncSecondExpr(a) => UnaryExpressionCodeGen(e, x => q"$truncTime($cru.SECONDS)($x)")

      case e @ ExtractYearExpr(a) => UnaryExpressionCodeGen(e, x => q"$x.toLocalDateTime.getYear")
      case e @ ExtractQuarterExpr(a) =>
        UnaryExpressionCodeGen(e, x => q"1 + ($x.toLocalDateTime.getMonth.getValue - 1) / 3")
      case e @ ExtractMonthExpr(a)  => UnaryExpressionCodeGen(e, x => q"$x.toLocalDateTime.getMonthValue")
      case e @ ExtractDayExpr(a)    => UnaryExpressionCodeGen(e, x => q"$x.toLocalDateTime.getDayOfMonth")
      case e @ ExtractHourExpr(a)   => UnaryExpressionCodeGen(e, x => q"$x.toLocalDateTime.getHour")
      case e @ ExtractMinuteExpr(a) => UnaryExpressionCodeGen(e, x => q"$x.toLocalDateTime.getMinute")
      case e @ ExtractSecondExpr(a) => UnaryExpressionCodeGen(e, x => q"$x.toLocalDateTime.getSecond")

      case e @ TimeMinusExpr(a, b) =>
        BinaryExpressionCodeGen(e, (x, y) => q"_root_.scala.math.abs($x.millis - $y.millis)")
      case e @ TimeMinusPeriodExpr(a, b) =>
        BinaryExpressionCodeGen(e, (t, p) => q"Time($t.toDateTime.minus($p))")
      case e @ TimePlusPeriodExpr(a, b)   => BinaryExpressionCodeGen(e, (t, p) => q"Time($t.toDateTime.plus($p))")
      case e @ PeriodPlusPeriodExpr(a, b) => BinaryExpressionCodeGen(e, (x, y) => q"$x plus $y")

      case e @ IsNullExpr(a)    => UnaryExpressionCodeGen(e, _ => q"false", Some(q"true"))
      case e @ IsNotNullExpr(a) => UnaryExpressionCodeGen(e, _ => q"true", Some(q"false"))

      case e @ LowerExpr(a) => UnaryExpressionCodeGen(e, x => q"$x.toLowerCase")
      case e @ UpperExpr(a) => UnaryExpressionCodeGen(e, x => q"$x.toUpperCase")

      case e @ ConditionExpr(_, _, _) => ConditionExpressionCodeGen(e)

      case e @ AbsExpr(_)        => MathUnaryExpressionCodeGen(e, TermName("abs"))
      case e @ UnaryMinusExpr(_) => MathUnaryExpressionCodeGen(e, TermName("negate"))

      case e @ NotExpr(a) => UnaryExpressionCodeGen(e, x => q"!$x")
      case e @ AndExpr(_) => LogicalExpressionCodeGen.and(e)
      case e @ OrExpr(_)  => LogicalExpressionCodeGen.or(e)

      case e @ TokensExpr(a)    => UnaryExpressionCodeGen(e, x => q"$tokenizer.transliteratedTokens($x)")
      case e @ SplitExpr(a)     => UnaryExpressionCodeGen(e, x => q"$calculator.splitBy($x, !_.isLetterOrDigit).toSeq")
      case e @ LengthExpr(a)    => UnaryExpressionCodeGen(e, x => q"$x.length")
      case e @ ConcatExpr(a, b) => BinaryExpressionCodeGen(e, (x, y) => q"$x + $y")

      case e @ ArrayExpr(_) => ArrayExpressionCodeGen(e)

      case e @ ArrayLengthExpr(a)   => UnaryExpressionCodeGen(e, x => q"$x.size")
      case e @ ArrayToStringExpr(a) => UnaryExpressionCodeGen(e, x => q"""$x.mkString(", ")""")
      case e @ ArrayTokensExpr(a) =>
        UnaryExpressionCodeGen(e, x => q"""$x.flatMap(s => $tokenizer.transliteratedTokens(s))""")

      case e @ ContainsExpr(as, b) => BinaryExpressionCodeGen(e, (x, y) => q"$x.contains($y)")
      case e @ ContainsAnyExpr(as, bs) =>
        BinaryExpressionCodeGen(e, (x, y) => q"$y.exists($x.contains)")
      case e @ ContainsAllExpr(as, bs) =>
        BinaryExpressionCodeGen(e, (x, y) => q"$y.forall($x.contains)")
      case e @ ContainsSameExpr(as, bs) =>
        BinaryExpressionCodeGen(e, (x, y) => q"$x.size == $y.size && $x.toSet == $y.toSet")
    }
  }
}
