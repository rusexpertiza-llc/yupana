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

package org.yupana.api.query

import org.yupana.api.{ Currency, Time }
import org.yupana.api.query.Expression.{ Condition, Transform }
import org.yupana.api.schema.{ Dimension, ExternalLink, LinkField, Metric }
import org.yupana.api.types.DataType.TypeKind
import org.yupana.api.types._
import org.yupana.api.types.guards.{ DivGuard, MinusGuard, PlusGuard, TimesGuard }
import org.yupana.api.utils.{ CollectionUtils, SortedSetIterator }

import scala.annotation.implicitNotFound

sealed trait Expression[Out] extends Serializable {
  val dataType: DataType.Aux[Out]

  val kind: ExprKind

  val isNullable: Boolean

  def as(name: String): QueryField = QueryField(name, this)

  def encode: String

  def fold[O](z: O)(f: (O, Expression[_]) => O): O

  def flatten: Set[Expression[_]] = fold(Set.empty[Expression[_]])(_ + _)

  def transform(f: Transform): Expression[Out]

  private val encoded: String = encode
  private val encodedHashCode: Int = encoded.hashCode()

  override def toString: String = encoded

  override def hashCode(): Int = encodedHashCode

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case that: Expression[_] => this.encoded == that.encoded
      case _                   => false
    }
  }
}

object Expression {
  type Condition = Expression[Boolean]

  trait Transform {
    def apply[T](x: Expression[T]): Option[Expression[T]]
    def applyOrDefault[T](x: Expression[T], default: => Expression[T]): Expression[T] = apply(x).getOrElse(default)
  }
}

sealed abstract class WindowFunctionExpr[In, Out](
    val expr: Expression[In],
    name: String,
    create: Expression[In] => Expression[Out]
) extends UnaryOperationExpr[In, Out](expr, name, create) {
  override val kind: ExprKind = if (expr.kind == Simple || expr.kind == Const) Window else Invalid
  override def encode: String = s"winFunc($name,${expr.encode})"
}

final case class LagExpr[I](override val expr: Expression[I])
    extends WindowFunctionExpr[I, I](expr, "lag", LagExpr(_)) {
  override val dataType: DataType.Aux[I] = expr.dataType
  override val isNullable: Boolean = true
}

sealed abstract class AggregateExpr[In, M, Out](
    val expr: Expression[In],
    val name: String,
    create: Expression[In] => Expression[Out]
) extends UnaryOperationExpr[In, Out](expr, name, create) {

  override val kind: ExprKind = if (expr.kind == Simple || expr.kind == Const) Aggregate else Invalid

  override def encode: String = s"agg($name,${expr.encode})"
}

final case class MinExpr[I](override val expr: Expression[I])(implicit val ord: Ordering[I])
    extends AggregateExpr[I, I, I](expr, "min", MinExpr(_)) {
  override val dataType: DataType.Aux[I] = expr.dataType
  override val isNullable: Boolean = expr.isNullable
}

final case class MaxExpr[I](override val expr: Expression[I])(implicit val ord: Ordering[I])
    extends AggregateExpr[I, I, I](expr, "max", MaxExpr(_)) {
  override val dataType: DataType.Aux[I] = expr.dataType
  override val isNullable: Boolean = expr.isNullable
}

final case class SumExpr[In, Out](override val expr: Expression[In])(
    implicit val plus: PlusGuard[Out, Out, Out],
    val dt: DataType.Aux[Out],
    @implicitNotFound("Unsupported sum expressions for types: ${In}, ${Out}")
    val guard: SumExpr.SumGuard[In, Out]
) extends AggregateExpr[In, In, Out](expr, "sum", SumExpr(_)) {
  override val dataType: DataType.Aux[Out] = dt
  override val isNullable: Boolean = expr.isNullable
}

object SumExpr {
  final class SumGuard[In, Out] extends Serializable

  implicit val sumInt: SumGuard[Int, Int] = new SumGuard[Int, Int]
  implicit val sumLong: SumGuard[Long, Long] = new SumGuard[Long, Long]
  implicit val sumDouble: SumGuard[Double, Double] = new SumGuard[Double, Double]
  implicit val sumFloat: SumGuard[Float, Float] = new SumGuard[Float, Float]
  implicit val sumByte: SumGuard[Byte, Int] = new SumGuard[Byte, Int]
  implicit val sumShort: SumGuard[Short, Int] = new SumGuard[Short, Int]
  implicit val sumBigDecimal: SumGuard[BigDecimal, BigDecimal] = new SumGuard[BigDecimal, BigDecimal]
  implicit val sumCurrency: SumGuard[Currency, Currency] = new SumGuard[Currency, Currency]
}

final case class AvgExpr[I](override val expr: Expression[I])(implicit val numeric: Num[I])
    extends AggregateExpr[I, I, BigDecimal](expr, "avg", AvgExpr(_)) {
  override val dataType: DataType.Aux[BigDecimal] = DataType[BigDecimal]
  override val isNullable: Boolean = expr.isNullable
}

final case class CountExpr[I](override val expr: Expression[I])
    extends AggregateExpr[I, Long, Long](expr, "count", CountExpr(_)) {
  override val dataType: DataType.Aux[Long] = DataType[Long]
  override val isNullable: Boolean = false
}

final case class DistinctCountExpr[I](override val expr: Expression[I])
    extends AggregateExpr[I, Set[I], Int](expr, "distinct_count", DistinctCountExpr(_)) {
  override val dataType: DataType.Aux[Int] = DataType[Int]
  override val isNullable: Boolean = false
}

final case class HLLCountExpr[I](override val expr: Expression[I], accuracy: Double)
    extends AggregateExpr[I, Set[I], Long](expr, "hll_count", HLLCountExpr(_, accuracy)) {
  override val dataType: DataType.Aux[Long] = DataType[Long]
  override val isNullable: Boolean = false
}

final case class DistinctRandomExpr[I](override val expr: Expression[I])
    extends AggregateExpr[I, Set[I], I](expr, "distinct_random", DistinctRandomExpr(_)) {
  override val dataType: DataType.Aux[I] = expr.dataType
}

sealed trait ValueExpr[T] extends Expression[T] {
  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = f(z, this)
  override def transform(f: Transform): Expression[T] = f.applyOrDefault(this, this)
}

sealed trait ConstExpr[T] extends ValueExpr[T] {
  override val kind: ExprKind = Const
}

final case class NullExpr[T](override val dataType: DataType.Aux[T]) extends ConstExpr[T] {
  override val isNullable: Boolean = true
  override def encode: String = "null"
}

final case class ConstantExpr[T](v: T)(implicit override val dataType: DataType.Aux[T]) extends ConstExpr[T] {

  override val isNullable: Boolean = false

  override def encode: String = {
    if (dataType.kind == TypeKind.Array) {
      val adt = dataType.asInstanceOf[ArrayDataType[T]]
      val vStr = v.asInstanceOf[adt.T].map(_.toString.replaceAll(",", "\\\\,")).mkString(",")
      s"const([$vStr]:${v.getClass.getSimpleName})"
    } else {
      s"const($v:${v.getClass.getSimpleName})"
    }
  }
}

case class TupleValueExpr[T, U](v1: ValueExpr[T], v2: ValueExpr[U]) extends ValueExpr[(T, U)] {
  override val dataType: DataType.Aux[(T, U)] = DataType.tupleDt(v1.dataType, v2.dataType)
  override val kind: ExprKind = ExprKind.combine(v1.kind, v2.kind)
  override val isNullable: Boolean = false
  override def encode: String = s"(${v1.encode}, ${v2.encode})"
}

final case class PlaceholderExpr[T](id: Int, override val dataType: DataType.Aux[T]) extends ValueExpr[T] {
  override val kind: ExprKind = Simple
  override val isNullable: Boolean = true
  override def encode: String = s"?$id:${dataType.meta.javaTypeName}"
}

final case class UntypedPlaceholderExpr(id: Int) extends ValueExpr[Null] {
  override val kind: ExprKind = Simple
  override val dataType: DataType.Aux[Null] = DataType[Null]
  override val isNullable: Boolean = true
  override def encode: String = s"?$id"
}

sealed trait SimpleExpr[T] extends Expression[T] {
  override val kind: ExprKind = Simple
  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = f(z, this)
  override def transform(f: Transform): Expression[T] = f.applyOrDefault(this, this)
}

case object NowExpr extends SimpleExpr[Time] {
  override val dataType: DataType.Aux[Time] = DataType[Time]
  override val isNullable: Boolean = false
  override def encode: String = "now()"
}

case object TimeExpr extends SimpleExpr[Time] {
  override val dataType: DataType.Aux[Time] = DataType[Time]
  override val isNullable: Boolean = false

  override def encode: String = s"time()"
  def toField: QueryField = QueryField("time", this)
}

case class DimensionExpr[T](dimension: Dimension.Aux[T]) extends SimpleExpr[T] {
  override val dataType: DataType.Aux[dimension.T] = dimension.dataType
  override val isNullable: Boolean = false

  override def encode: String = s"dim(${dimension.name})"
  def toField: QueryField = QueryField(dimension.name, this)
}

case class DimensionIdExpr(dimension: Dimension) extends SimpleExpr[String] {
  override val dataType: DataType.Aux[String] = DataType[String]
  override val isNullable: Boolean = false

  override def encode: String = s"dimId(${dimension.name})"
  def toField: QueryField = QueryField(dimension.name, this)
}

final case class MetricExpr[T](metric: Metric.Aux[T]) extends SimpleExpr[T] {
  override val dataType: DataType.Aux[metric.T] = metric.dataType
  override val isNullable: Boolean = true

  override def encode: String = s"metric(${metric.name})"
  def toField: QueryField = QueryField(metric.name, this)
}

final case class LinkExpr[T](link: ExternalLink, linkField: LinkField.Aux[T]) extends SimpleExpr[T] {
  override val dataType: DataType.Aux[linkField.T] = linkField.dataType
  override val isNullable: Boolean = true

  override def encode: String = s"link(${link.linkName}, ${linkField.name})"
  private def queryFieldName: String = link.linkName + "_" + linkField.name
  def toField: QueryField = QueryField(queryFieldName, this)
}

object LinkExpr {
  def apply(link: ExternalLink, field: String): LinkExpr[String] = new LinkExpr(link, LinkField[String](field))
}

sealed abstract class UnaryOperationExpr[In, Out](
    expr: Expression[In],
    functionName: String,
    create: Expression[In] => Expression[Out]
) extends Expression[Out] {
  override val dataType: DataType.Aux[Out]
  override val kind: ExprKind = expr.kind
  override val isNullable: Boolean = expr.isNullable
  def operand: Expression[In] = expr

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = expr.fold(f(z, this))(f)
  override def transform(f: Transform): Expression[Out] = f.applyOrDefault(this, create(expr.transform(f)))

  override def encode: String = s"$functionName(${expr.encode})"
  override def toString: String = s"$functionName($expr)"
}

final case class UnaryMinusExpr[N](expr: Expression[N])(implicit val num: Num[N])
    extends UnaryOperationExpr[N, N](expr, "-", UnaryMinusExpr(_)) {
  override val dataType: DataType.Aux[N] = expr.dataType
}

final case class AbsExpr[N](expr: Expression[N])(implicit val num: Num[N])
    extends UnaryOperationExpr[N, N](expr, "abs", AbsExpr(_)) {
  override val dataType: DataType.Aux[N] = expr.dataType
}

final case class NotExpr(expr: Expression[Boolean])
    extends UnaryOperationExpr[Boolean, Boolean](expr, "not", NotExpr)
    with SimpleCondition

final case class LengthExpr(expr: Expression[String])
    extends UnaryOperationExpr[String, Int](expr, "length", LengthExpr) {
  override val dataType: DataType.Aux[Int] = DataType[Int]
}

final case class LowerExpr(expr: Expression[String])
    extends UnaryOperationExpr[String, String](expr, "lower", LowerExpr) {
  override val dataType: DataType.Aux[String] = DataType[String]
}

final case class UpperExpr(expr: Expression[String])
    extends UnaryOperationExpr[String, String](expr, "upper", UpperExpr) {
  override val dataType: DataType.Aux[String] = DataType[String]
}

final case class TokensExpr(expr: Expression[String])
    extends UnaryOperationExpr[String, Seq[String]](expr, "tokens", TokensExpr) {
  override val dataType: DataType.Aux[Seq[String]] = DataType[Seq[String]]
}

final case class ArrayTokensExpr(expr: Expression[Seq[String]])
    extends UnaryOperationExpr[Seq[String], Seq[String]](expr, "tokens", ArrayTokensExpr) {
  override val dataType: DataType.Aux[Seq[String]] = DataType[Seq[String]]
}

final case class SplitExpr(expr: Expression[String])
    extends UnaryOperationExpr[String, Seq[String]](expr, "split", SplitExpr) {
  override val dataType: DataType.Aux[Seq[String]] = DataType[Seq[String]]
}

final case class ArrayToStringExpr[T](expr: Expression[Seq[T]])
    extends UnaryOperationExpr[Seq[T], String](expr, "array_to_string", ArrayToStringExpr(_)) {
  override val dataType: DataType.Aux[String] = DataType[String]
}

final case class ArrayLengthExpr[T](expr: Expression[Seq[T]])
    extends UnaryOperationExpr[Seq[T], Int](expr, "length", ArrayLengthExpr(_)) {
  override val dataType: DataType.Aux[Int] = DataType[Int]
}

final case class ExtractYearExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Int](expr, "extractYear", ExtractYearExpr) {
  override val dataType: DataType.Aux[Int] = DataType[Int]
}

final case class ExtractQuarterExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Int](expr, "extractQuarter", ExtractQuarterExpr) {
  override val dataType: DataType.Aux[Int] = DataType[Int]
}

final case class ExtractMonthExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Int](expr, "extractMonth", ExtractMonthExpr) {
  override val dataType: DataType.Aux[Int] = DataType[Int]
}

final case class ExtractDayExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Int](expr, "extractDay", ExtractDayExpr) {
  override val dataType: DataType.Aux[Int] = DataType[Int]
}

final case class ExtractHourExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Int](expr, "extractHour", ExtractHourExpr) {
  override val dataType: DataType.Aux[Int] = DataType[Int]
}

final case class ExtractMinuteExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Int](expr, "extractMinute", ExtractMinuteExpr) {
  override val dataType: DataType.Aux[Int] = DataType[Int]
}

final case class ExtractSecondExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Int](expr, "extractSecond", ExtractSecondExpr) {
  override val dataType: DataType.Aux[Int] = DataType[Int]
}

final case class TruncYearExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Time](expr, "truncYear", TruncYearExpr) {
  override val dataType: DataType.Aux[Time] = DataType[Time]
}

final case class TruncQuarterExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Time](expr, "truncQuarter", TruncQuarterExpr) {
  override val dataType: DataType.Aux[Time] = DataType[Time]
}

final case class TruncMonthExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Time](expr, "truncMonth", TruncMonthExpr) {
  override val dataType: DataType.Aux[Time] = DataType[Time]
}

final case class TruncWeekExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Time](expr, "truncWeek", TruncWeekExpr) {
  override val dataType: DataType.Aux[Time] = DataType[Time]
}

final case class TruncDayExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Time](expr, "truncDay", TruncDayExpr) {
  override val dataType: DataType.Aux[Time] = DataType[Time]
}

final case class TruncHourExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Time](expr, "truncHour", TruncHourExpr) {
  override val dataType: DataType.Aux[Time] = DataType[Time]
}

final case class TruncMinuteExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Time](expr, "truncMinute", TruncMinuteExpr) {
  override val dataType: DataType.Aux[Time] = DataType[Time]
}

final case class TruncSecondExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Time](expr, "truncSecond", TruncSecondExpr) {
  override val dataType: DataType.Aux[Time] = DataType[Time]
}

final case class IsNullExpr[T](expr: Expression[T])
    extends UnaryOperationExpr[T, Boolean](expr, "isNull", IsNullExpr(_))
    with SimpleCondition

final case class IsNotNullExpr[T](expr: Expression[T])
    extends UnaryOperationExpr[T, Boolean](expr, "isNotNull", IsNotNullExpr(_))
    with SimpleCondition

sealed abstract class BinaryOperationExpr[T, U, Out](
    val a: Expression[T],
    val b: Expression[U],
    val functionName: String,
    isInfix: Boolean,
    create: (Expression[T], Expression[U]) => Expression[Out]
) extends Expression[Out] {

  def operandA: Expression[T] = a
  def operandB: Expression[U] = b

  override val isNullable: Boolean = a.isNullable || b.isNullable

  override def fold[B](z: B)(f: (B, Expression[_]) => B): B = {
    val z1 = a.fold(f(z, this))(f)
    b.fold(z1)(f)
  }

  override def transform(f: Transform): Expression[Out] =
    f.applyOrDefault(this, create(a.transform(f), b.transform(f)))

  override def toString: String = if (isInfix) s"$a $functionName $b" else s"$functionName($a, $b)"
  override def encode: String = s"$functionName(${a.encode}, ${b.encode})"

  override val kind: ExprKind = ExprKind.combine(a.kind, b.kind)
}

sealed trait SimpleCondition extends Expression[Boolean] {
  override val dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

case object TrueExpr extends SimpleCondition with ConstExpr[Boolean] {
  override val isNullable: Boolean = false

  override def encode: String = "true"
}

case object FalseExpr extends SimpleCondition with ConstExpr[Boolean] {
  override val isNullable: Boolean = false
  override def encode: String = "false"
}

final case class EqExpr[T](override val a: Expression[T], override val b: Expression[T])
    extends BinaryOperationExpr[T, T, Boolean](a, b, "=", isInfix = true, EqExpr.apply)
    with SimpleCondition

final case class NeqExpr[T](override val a: Expression[T], override val b: Expression[T])
    extends BinaryOperationExpr[T, T, Boolean](a, b, "<>", isInfix = true, NeqExpr.apply)
    with SimpleCondition

final case class LtExpr[T](override val a: Expression[T], override val b: Expression[T])(
    implicit val ordering: Ordering[T]
) extends BinaryOperationExpr[T, T, Boolean](a, b, "<", isInfix = true, LtExpr.apply)
    with SimpleCondition

final case class GtExpr[T](override val a: Expression[T], override val b: Expression[T])(
    implicit val ordering: Ordering[T]
) extends BinaryOperationExpr[T, T, Boolean](a, b, ">", isInfix = true, GtExpr.apply)
    with SimpleCondition

final case class LeExpr[T](override val a: Expression[T], override val b: Expression[T])(
    implicit val ordering: Ordering[T]
) extends BinaryOperationExpr[T, T, Boolean](a, b, "<=", isInfix = true, LeExpr.apply)
    with SimpleCondition

final case class GeExpr[T](override val a: Expression[T], override val b: Expression[T])(
    implicit val ordering: Ordering[T]
) extends BinaryOperationExpr[T, T, Boolean](a, b, ">=", isInfix = true, GeExpr.apply)
    with SimpleCondition

final case class PlusExpr[A, B, R](override val a: Expression[A], override val b: Expression[B])(
    implicit val guard: PlusGuard[A, B, R]
) extends BinaryOperationExpr[A, B, R](a, b, "+", isInfix = true, PlusExpr.apply) {
  override val dataType: DataType.Aux[R] = guard.dataType
}

final case class MinusExpr[A, B, R](override val a: Expression[A], override val b: Expression[B])(
    implicit val guard: MinusGuard[A, B, R]
) extends BinaryOperationExpr[A, B, R](a, b, "-", isInfix = true, MinusExpr.apply) {
  override val dataType: DataType.Aux[R] = guard.dataType
}

final case class TimesExpr[A, B, R](override val a: Expression[A], override val b: Expression[B])(
    implicit val guard: TimesGuard[A, B, R]
) extends BinaryOperationExpr[A, B, R](a, b, "*", isInfix = true, TimesExpr.apply) {
  override val dataType: DataType.Aux[R] = guard.dataType
}

final case class DivExpr[A, B, R](override val a: Expression[A], override val b: Expression[B])(
    implicit val dg: DivGuard[A, B, R]
) extends BinaryOperationExpr[A, B, R](a, b, "/", isInfix = true, DivExpr.apply) {
  override val dataType: DataType.Aux[R] = dg.dataType
}

//final case class TimeMinusExpr(override val a: Expression[Time], override val b: Expression[Time])
//    extends BinaryOperationExpr[Time, Time, Long](a, b, "-", isInfix = true) {
//  override val dataType: DataType.Aux[Long] = DataType[Long]
//}
//
//final case class TimeMinusPeriodExpr(override val a: Expression[Time], override val b: Expression[PeriodDuration])
//    extends BinaryOperationExpr[Time, PeriodDuration, Time](a, b, "-", isInfix = true) {
//  override val dataType: DataType.Aux[Time] = DataType[Time]
//}
//
//final case class TimePlusPeriodExpr(override val a: Expression[Time], override val b: Expression[PeriodDuration])
//    extends BinaryOperationExpr[Time, PeriodDuration, Time](a, b, "+", isInfix = true) {
//  override val dataType: DataType.Aux[Time] = DataType[Time]
//}
//
//final case class PeriodPlusPeriodExpr(
//    override val a: Expression[PeriodDuration],
//    override val b: Expression[PeriodDuration]
//) extends BinaryOperationExpr[PeriodDuration, PeriodDuration, PeriodDuration](a, b, "+", isInfix = true) {
//  override val dataType: DataType.Aux[PeriodDuration] = DataType[PeriodDuration]
//}
//
//final case class ConcatExpr(override val a: Expression[String], override val b: Expression[String])
//    extends BinaryOperationExpr[String, String, String](a, b, "+", isInfix = true) {
//  override val dataType: DataType.Aux[String] = DataType[String]
//}

final case class ContainsExpr[T](override val a: Expression[Seq[T]], override val b: Expression[T])
    extends BinaryOperationExpr[Seq[T], T, Boolean](a, b, "contains", isInfix = false, ContainsExpr.apply) {

  override val dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

final case class ContainsAllExpr[T](override val a: Expression[Seq[T]], override val b: Expression[Seq[T]])
    extends BinaryOperationExpr[Seq[T], Seq[T], Boolean](a, b, "containsAll", isInfix = false, ContainsAllExpr.apply) {
  override val dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

final case class ContainsAnyExpr[T](override val a: Expression[Seq[T]], override val b: Expression[Seq[T]])
    extends BinaryOperationExpr[Seq[T], Seq[T], Boolean](a, b, "containsAny", isInfix = false, ContainsAnyExpr.apply) {
  override val dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

final case class ContainsSameExpr[T](override val a: Expression[Seq[T]], override val b: Expression[Seq[T]])
    extends BinaryOperationExpr[Seq[T], Seq[T], Boolean](
      a,
      b,
      "containsSame",
      isInfix = false,
      ContainsSameExpr.apply
    ) {
  override val dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

final case class TupleExpr[T, U](e1: Expression[T], e2: Expression[U]) extends Expression[(T, U)] {
  override val dataType: DataType.Aux[(T, U)] = DataType.tupleDt(e1.dataType, e2.dataType)
  override val kind: ExprKind = ExprKind.combine(e1.kind, e2.kind)
  override val isNullable: Boolean = false

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = {
    val z1 = e1.fold(f(z, this))(f)
    e2.fold(z1)(f)
  }

  override def transform(f: Transform): Expression[(T, U)] =
    f.applyOrDefault(this, TupleExpr(e1.transform(f), e2.transform(f)))

  override def encode: String = s"(${e1.encode}, ${e2.encode})"
  override def toString: String = s"($e1, $e2)"
}

final case class ArrayExpr[T](exprs: Seq[Expression[T]])(implicit val elementDataType: DataType.Aux[T])
    extends Expression[Seq[T]] {
  override val dataType: DataType.Aux[Seq[T]] = DataType[Seq[T]]
  override val kind: ExprKind = exprs.foldLeft(Const: ExprKind)((a, e) => ExprKind.combine(a, e.kind))
  override val isNullable: Boolean = false

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = exprs.foldLeft(f(z, this))((a, e) => e.fold(a)(f))

  override def transform(f: Transform): Expression[Seq[T]] =
    f.applyOrDefault(this, ArrayExpr(exprs.map(_.transform(f))))

  override def encode: String = exprs.map(_.encode).mkString("[", ", ", "]")
  override def toString: String = CollectionUtils.mkStringWithLimit(exprs)
}

final case class ConditionExpr[T](
    condition: Condition,
    positive: Expression[T],
    negative: Expression[T]
) extends Expression[T] {
  override val dataType: DataType.Aux[T] = positive.dataType
  override val kind: ExprKind = ExprKind.combine(condition.kind, ExprKind.combine(positive.kind, negative.kind))
  override val isNullable: Boolean = positive.isNullable || negative.isNullable

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = {
    val z1 = condition.fold(f(z, this))(f)
    val z2 = positive.fold(z1)(f)
    negative.fold(z2)(f)
  }

  override def transform(f: Transform): Expression[T] =
    f.applyOrDefault(this, ConditionExpr(condition.transform(f), positive.transform(f), negative.transform(f)))

  override def toString: String = s"IF ($condition) THEN $positive ELSE $negative"
  override def encode: String = s"if(${condition.encode},${positive.encode},${negative.encode}"
}

final case class InExpr[T](expr: Expression[T], values: Set[ValueExpr[T]])
    extends UnaryOperationExpr[T, Boolean](expr, "in", InExpr(_, values))
    with SimpleCondition {

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O =
    values.foldLeft(expr.fold(f(z, this))(f))((e, v) => v.fold(e)(f))

  override def transform(f: Transform): Expression[Boolean] = {
    val vs: Set[ValueExpr[T]] = values.map(v =>
      f.apply(v) match {
        case Some(x: ValueExpr[_]) => x
        case _                     => v
      }
    )
    f.applyOrDefault(this, InExpr(expr.transform(f), vs))
  }

  override def encode: String = values.toSeq.map(_.toString).sorted.mkString(s"in(${expr.encode}, (", ",", "))")
  override def toString: String =
    expr.toString + CollectionUtils.mkStringWithLimit(values, 10, " IN (", ", ", ")")
}

final case class NotInExpr[T](expr: Expression[T], values: Set[ValueExpr[T]])
    extends UnaryOperationExpr[T, Boolean](expr, "notIn", NotInExpr(_, values))
    with SimpleCondition {

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O =
    values.foldLeft(expr.fold(f(z, this))(f))((e, v) => v.fold(e)(f))

  override def transform(f: Transform): Expression[Boolean] = {
    val vs: Set[ValueExpr[T]] = values.map(v =>
      f.apply(v) match {
        case Some(x: ValueExpr[_]) => x
        case _                     => v
      }
    )
    f.applyOrDefault(this, NotInExpr(expr.transform(f), vs))
  }

  override def encode: String = values.toSeq.map(_.toString).sorted.mkString(s"notIn(${expr.encode}, (", ",", "))")
  override def toString: String =
    expr.toString + CollectionUtils.mkStringWithLimit(values, 10, " NOT IN (", ", ", ")")
}

final case class DimIdInExpr[R](dim: Dimension.AuxR[R], values: SortedSetIterator[R])
    extends SimpleCondition
    with SimpleExpr[Boolean] {
  override val isNullable: Boolean = false

  override def encode: String = s"idIn($dim, (Iterator))"
  override def toString: String = s"$dim ID IN (Iterator)"

  override def equals(that: Any): Boolean = false
}

final case class DimIdNotInExpr[R](dim: Dimension.AuxR[R], values: SortedSetIterator[R])
    extends SimpleCondition
    with SimpleExpr[Boolean] {
  override val isNullable: Boolean = false

  override def encode: String = s"idNotIn($dim, (Iterator))"
  override def toString: String = s"$dim ID NOT IN (Iterator)"

  override def equals(that: Any): Boolean = false
}

final case class AndExpr(conditions: Seq[Condition]) extends Expression[Boolean] {
  override val dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override val kind: ExprKind = conditions.foldLeft(Const: ExprKind)((k, c) => ExprKind.combine(k, c.kind))
  override val isNullable: Boolean = false

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = conditions.foldLeft(f(z, this))((a, e) => e.fold(a)(f))

  override def transform(f: Transform): Expression[Boolean] =
    f.applyOrDefault(this, AndExpr(conditions.map(_.transform(f))))

  override def toString: String = conditions.mkString("(", " AND ", ")")
  override def encode: String = conditions.map(_.encode).sorted.mkString("and(", ",", ")")
}

final case class OrExpr(conditions: Seq[Condition]) extends Expression[Boolean] {
  override val dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override val kind: ExprKind = conditions.foldLeft(Const: ExprKind)((k, c) => ExprKind.combine(k, c.kind))
  override val isNullable: Boolean = false

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = conditions.foldLeft(f(z, this))((a, e) => e.fold(a)(f))
  override def transform(f: Transform): Expression[Boolean] =
    f.applyOrDefault(this, OrExpr(conditions.map(_.transform(f))))

  override def toString: String = conditions.mkString("(", " OR ", ")")
  override def encode: String = conditions.map(_.encode).sorted.mkString("or(", ",", ")")
}

sealed abstract class TypeConvertExpr[T, U](expr: Expression[T], create: Expression[T] => Expression[U])(
    implicit dtt: DataType.Aux[T],
    dtu: DataType.Aux[U]
) extends UnaryOperationExpr[T, U](
      expr,
      dtt.meta.sqlTypeName.toLowerCase + "2" + dtu.meta.sqlTypeName.toLowerCase,
      create
    ) {

  override val dataType: DataType.Aux[U] = dtu
}

final case class Double2BigDecimalExpr(expr: Expression[Double])
    extends TypeConvertExpr[Double, BigDecimal](expr, Double2BigDecimalExpr)
//final case class Double2CurrencyExpr(expr: Expression[Double]) extends TypeConvertExpr[Double, Currency](expr)

final case class BigDecimal2DoubleExpr(expr: Expression[BigDecimal])
    extends TypeConvertExpr[BigDecimal, Double](expr, BigDecimal2DoubleExpr)
//final case class Currency2DoubleExpr(expr: Expression[Currency]) extends TypeConvertExpr[Currency, Double](expr)

final case class Long2BigDecimalExpr(expr: Expression[Long])
    extends TypeConvertExpr[Long, BigDecimal](expr, Long2BigDecimalExpr)
//final case class Long2CurrencyExpr(expr: Expression[Long]) extends TypeConvertExpr[Long, Currency](expr)
final case class Long2DoubleExpr(expr: Expression[Long]) extends TypeConvertExpr[Long, Double](expr, Long2DoubleExpr)

final case class Int2LongExpr(expr: Expression[Int]) extends TypeConvertExpr[Int, Long](expr, Int2LongExpr)
final case class Int2BigDecimalExpr(expr: Expression[Int])
    extends TypeConvertExpr[Int, BigDecimal](expr, Int2BigDecimalExpr)
final case class Int2DoubleExpr(expr: Expression[Int]) extends TypeConvertExpr[Int, Double](expr, Int2DoubleExpr)

final case class Short2IntExpr(expr: Expression[Short]) extends TypeConvertExpr[Short, Int](expr, Short2IntExpr)
final case class Short2LongExpr(expr: Expression[Short]) extends TypeConvertExpr[Short, Long](expr, Short2LongExpr)
final case class Short2BigDecimalExpr(expr: Expression[Short])
    extends TypeConvertExpr[Short, BigDecimal](expr, Short2BigDecimalExpr)
final case class Short2DoubleExpr(expr: Expression[Short])
    extends TypeConvertExpr[Short, Double](expr, Short2DoubleExpr)

final case class Byte2ShortExpr(expr: Expression[Byte]) extends TypeConvertExpr[Byte, Short](expr, Byte2ShortExpr)
final case class Byte2IntExpr(expr: Expression[Byte]) extends TypeConvertExpr[Byte, Int](expr, Byte2IntExpr)
final case class Byte2LongExpr(expr: Expression[Byte]) extends TypeConvertExpr[Byte, Long](expr, Byte2LongExpr)
final case class Byte2BigDecimalExpr(expr: Expression[Byte])
    extends TypeConvertExpr[Byte, BigDecimal](expr, Byte2BigDecimalExpr)
final case class Byte2DoubleExpr(expr: Expression[Byte]) extends TypeConvertExpr[Byte, Double](expr, Byte2DoubleExpr)

final case class ToStringExpr[T](expr: Expression[T])(implicit dt: DataType.Aux[T])
    extends TypeConvertExpr[T, String](expr, ToStringExpr(_))
