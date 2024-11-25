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

import org.threeten.extra.PeriodDuration
import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.schema.{ Dimension, ExternalLink, LinkField, Metric }
import org.yupana.api.types.DataType.TypeKind
import org.yupana.api.types._
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

  def transform(f: PartialFunction[Expression[Out], Expression[Out]]): Expression[Out]

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
}

sealed abstract class WindowFunctionExpr[In, Out, Self <: Expression[Out]](
    val expr: Expression[In],
    name: String,
    create: Expression[In] => Self
) extends UnaryOperationExpr[In, Out, Self](expr, name, create) {
  override val kind: ExprKind = if (expr.kind == Simple || expr.kind == Const) Window else Invalid
  override def encode: String = s"winFunc($name,${expr.encode})"
}

final case class LagExpr[I](override val expr: Expression[I])
    extends WindowFunctionExpr[I, I, LagExpr[I]](expr, "lag", LagExpr(_)) {
  override val dataType: DataType.Aux[I] = expr.dataType
  override val isNullable: Boolean = true
}

sealed abstract class AggregateExpr[In, M, Out, Self <: Expression[Out]](
    val expr: Expression[In],
    val name: String,
    create: Expression[In] => Self
) extends UnaryOperationExpr[In, Out, Self](expr, name, create) {

  override val kind: ExprKind = if (expr.kind == Simple || expr.kind == Const) Aggregate else Invalid

  override def encode: String = s"agg($name,${expr.encode})"
}

final case class MinExpr[I](override val expr: Expression[I])(implicit val ord: Ordering[I])
    extends AggregateExpr[I, I, I, MinExpr[I]](expr, "min", MinExpr(_)) {
  override val dataType: DataType.Aux[I] = expr.dataType
  override val isNullable: Boolean = expr.isNullable
}

final case class MaxExpr[I](override val expr: Expression[I])(implicit val ord: Ordering[I])
    extends AggregateExpr[I, I, I, MaxExpr[I]](expr, "max", MaxExpr(_)) {
  override val dataType: DataType.Aux[I] = expr.dataType
  override val isNullable: Boolean = expr.isNullable
}

final case class SumExpr[In, Out](override val expr: Expression[In])(
    implicit val numeric: Numeric[Out],
    implicit val dt: DataType.Aux[Out],
    @implicitNotFound("Unsupported sum expressions for types: ${In}, ${Out}")
    implicit val guard: SumExpr.SumGuard[In, Out]
) extends AggregateExpr[In, In, Out, SumExpr[In, Out]](expr, "sum", SumExpr(_)) {
  override val dataType: DataType.Aux[Out] = dt
  override val isNullable: Boolean = expr.isNullable
}

object SumExpr {
  case class SumGuard[In, Out]()

  implicit val sumInt: SumGuard[Int, Int] = SumGuard[Int, Int]()
  implicit val sumLong: SumGuard[Long, Long] = SumGuard[Long, Long]()
  implicit val sumDouble: SumGuard[Double, Double] = SumGuard[Double, Double]()
  implicit val sumFloat: SumGuard[Float, Float] = SumGuard[Float, Float]()
  implicit val sumByte: SumGuard[Byte, Int] = SumGuard[Byte, Int]()
  implicit val sumShort: SumGuard[Short, Int] = SumGuard[Short, Int]()
  implicit val sumBigDecimal: SumGuard[BigDecimal, BigDecimal] = SumGuard[BigDecimal, BigDecimal]()
}

final case class AvgExpr[I](override val expr: Expression[I])(implicit val numeric: Numeric[I])
    extends AggregateExpr[I, I, BigDecimal, AvgExpr[I]](expr, "avg", AvgExpr(_)) {
  override val dataType: DataType.Aux[BigDecimal] = DataType[BigDecimal]
  override val isNullable: Boolean = expr.isNullable
}

final case class CountExpr[I](override val expr: Expression[I])
    extends AggregateExpr[I, Long, Long, CountExpr[I]](expr, "count", CountExpr(_)) {
  override val dataType: DataType.Aux[Long] = DataType[Long]
  override val isNullable: Boolean = false
}

final case class DistinctCountExpr[I](override val expr: Expression[I])
    extends AggregateExpr[I, Set[I], Int, DistinctCountExpr[I]](expr, "distinct_count", DistinctCountExpr(_)) {
  override val dataType: DataType.Aux[Int] = DataType[Int]
  override val isNullable: Boolean = false
}

final case class HLLCountExpr[I](override val expr: Expression[I], accuracy: Double)
    extends AggregateExpr[I, Set[I], Long, HLLCountExpr[I]](expr, "hll_count", HLLCountExpr(_, accuracy)) {
  override val dataType: DataType.Aux[Long] = DataType[Long]
  override val isNullable: Boolean = false
}

final case class DistinctRandomExpr[I](override val expr: Expression[I])
    extends AggregateExpr[I, Set[I], I, DistinctRandomExpr[I]](expr, "distinct_random", DistinctRandomExpr(_)) {
  override val dataType: DataType.Aux[I] = expr.dataType
}

sealed trait ConstExpr[T] extends Expression[T] {
  override val kind: ExprKind = Const
  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = f(z, this)
  override def transform(f: PartialFunction[Expression[T], Expression[T]]): Expression[T] =
    f.applyOrElse(this, identity[Expression[T]])
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

final case class PlaceholderExpr[T](id: Int, override val dataType: DataType.Aux[T]) extends ConstExpr[T] {
  override val isNullable: Boolean = true
  override def encode: String = s"?$id:${dataType.meta.javaTypeName}"
}

final case class UntypedPlaceholderExpr(id: Int) extends ConstExpr[Null] {
  override val dataType: DataType.Aux[Null] = DataType[Null]
  override val isNullable: Boolean = true
  override def encode: String = s"?$id"
}

sealed trait SimpleExpr[T] extends Expression[T] {
  override val kind: ExprKind = Simple
  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = f(z, this)
  override def transform(f: PartialFunction[Expression[T], Expression[T]]): Expression[T] =
    f.applyOrElse(this, identity[Expression[T]])
}

case object NowExpr extends Expression[Time] {
  override val dataType: DataType.Aux[Time] = DataType[Time]
  override val isNullable: Boolean = false
  override def encode: String = "now()"
  override val kind: ExprKind = Const
  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = f(z, this)
  override def transform(f: PartialFunction[Expression[Time], Expression[Time]]): Expression[Time] =
    f.applyOrElse(this, identity[Expression[Time]])
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

sealed abstract class UnaryOperationExpr[In, Out, Self <: Expression[Out]](
    expr: Expression[In],
    functionName: String,
    create: Expression[In] => Self
) extends Expression[Out] {
  override val dataType: DataType.Aux[Out]
  override val kind: ExprKind = expr.kind
  override val isNullable: Boolean = expr.isNullable
  def operand: Expression[In] = expr

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = expr.fold(f(z, this))(f)
  override def transform(f: PartialFunction[Expression[Out], Expression[Out]]): Expression[Out] = ???
//    f.applyOrElse(this, create(expr.transform(f)))

  override def encode: String = s"$functionName(${expr.encode})"
  override def toString: String = s"$functionName($expr)"
}

final case class UnaryMinusExpr[N](expr: Expression[N])(implicit val num: Numeric[N])
    extends UnaryOperationExpr[N, N, UnaryMinusExpr[N]](expr, "-", UnaryMinusExpr(_)) {
  override val dataType: DataType.Aux[N] = expr.dataType
}

final case class AbsExpr[N](expr: Expression[N])(implicit val num: Numeric[N])
    extends UnaryOperationExpr[N, N, AbsExpr[N]](expr, "abs", AbsExpr(_)) {
  override val dataType: DataType.Aux[N] = expr.dataType
}

final case class NotExpr(expr: Expression[Boolean])
    extends UnaryOperationExpr[Boolean, Boolean, NotExpr](expr, "not", NotExpr)
    with SimpleCondition

final case class LengthExpr(expr: Expression[String])
    extends UnaryOperationExpr[String, Int, LengthExpr](expr, "length", LengthExpr) {
  override val dataType: DataType.Aux[Int] = DataType[Int]
}

final case class LowerExpr(expr: Expression[String])
    extends UnaryOperationExpr[String, String, LowerExpr](expr, "lower", LowerExpr) {
  override val dataType: DataType.Aux[String] = DataType[String]
}

final case class UpperExpr(expr: Expression[String])
    extends UnaryOperationExpr[String, String, UpperExpr](expr, "upper", UpperExpr) {
  override val dataType: DataType.Aux[String] = DataType[String]
}

final case class TokensExpr(expr: Expression[String])
    extends UnaryOperationExpr[String, Seq[String], TokensExpr](expr, "tokens", TokensExpr) {
  override val dataType: DataType.Aux[Seq[String]] = DataType[Seq[String]]
}

final case class ArrayTokensExpr(expr: Expression[Seq[String]])
    extends UnaryOperationExpr[Seq[String], Seq[String], ArrayTokensExpr](expr, "tokens", ArrayTokensExpr) {
  override val dataType: DataType.Aux[Seq[String]] = DataType[Seq[String]]
}

final case class SplitExpr(expr: Expression[String])
    extends UnaryOperationExpr[String, Seq[String], SplitExpr](expr, "split", SplitExpr) {
  override val dataType: DataType.Aux[Seq[String]] = DataType[Seq[String]]
}

final case class ArrayToStringExpr[T](expr: Expression[Seq[T]])
    extends UnaryOperationExpr[Seq[T], String, ArrayToStringExpr[T]](expr, "array_to_string", ArrayToStringExpr(_)) {
  override val dataType: DataType.Aux[String] = DataType[String]
}

final case class ArrayLengthExpr[T](expr: Expression[Seq[T]])
    extends UnaryOperationExpr[Seq[T], Int, ArrayLengthExpr[T]](expr, "length", ArrayLengthExpr(_)) {
  override val dataType: DataType.Aux[Int] = DataType[Int]
}

final case class ExtractYearExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Int, ExtractYearExpr](expr, "extractYear", ExtractYearExpr) {
  override val dataType: DataType.Aux[Int] = DataType[Int]
}

final case class ExtractQuarterExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Int, ExtractQuarterExpr](expr, "extractQuarter", ExtractQuarterExpr) {
  override val dataType: DataType.Aux[Int] = DataType[Int]
}

final case class ExtractMonthExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Int, ExtractMonthExpr](expr, "extractMonth", ExtractMonthExpr) {
  override val dataType: DataType.Aux[Int] = DataType[Int]
}

final case class ExtractDayExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Int, ExtractDayExpr](expr, "extractDay", ExtractDayExpr) {
  override val dataType: DataType.Aux[Int] = DataType[Int]
}

final case class ExtractHourExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Int, ExtractHourExpr](expr, "extractHour", ExtractHourExpr) {
  override val dataType: DataType.Aux[Int] = DataType[Int]
}

final case class ExtractMinuteExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Int, ExtractMinuteExpr](expr, "extractMinute", ExtractMinuteExpr) {
  override val dataType: DataType.Aux[Int] = DataType[Int]
}

final case class ExtractSecondExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Int, ExtractSecondExpr](expr, "extractSecond", ExtractSecondExpr) {
  override val dataType: DataType.Aux[Int] = DataType[Int]
}

final case class TruncYearExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Time, TruncYearExpr](expr, "truncYear", TruncYearExpr) {
  override val dataType: DataType.Aux[Time] = DataType[Time]
}

final case class TruncQuarterExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Time, TruncQuarterExpr](expr, "truncQuarter", TruncQuarterExpr) {
  override val dataType: DataType.Aux[Time] = DataType[Time]
}

final case class TruncMonthExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Time, TruncMonthExpr](expr, "truncMonth", TruncMonthExpr) {
  override val dataType: DataType.Aux[Time] = DataType[Time]
}

final case class TruncWeekExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Time, TruncWeekExpr](expr, "truncWeek", TruncWeekExpr) {
  override val dataType: DataType.Aux[Time] = DataType[Time]
}

final case class TruncDayExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Time, TruncDayExpr](expr, "truncDay", TruncDayExpr) {
  override val dataType: DataType.Aux[Time] = DataType[Time]
}

final case class TruncHourExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Time, TruncHourExpr](expr, "truncHour", TruncHourExpr) {
  override val dataType: DataType.Aux[Time] = DataType[Time]
}

final case class TruncMinuteExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Time, TruncMinuteExpr](expr, "truncMinute", TruncMinuteExpr) {
  override val dataType: DataType.Aux[Time] = DataType[Time]
}

final case class TruncSecondExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Time, TruncSecondExpr](expr, "truncSecond", TruncSecondExpr) {
  override val dataType: DataType.Aux[Time] = DataType[Time]
}

final case class IsNullExpr[T](expr: Expression[T])
    extends UnaryOperationExpr[T, Boolean, IsNullExpr[T]](expr, "isNull", IsNullExpr(_))
    with SimpleCondition

final case class IsNotNullExpr[T](expr: Expression[T])
    extends UnaryOperationExpr[T, Boolean, IsNotNullExpr[T]](expr, "isNotNull", IsNotNullExpr(_))
    with SimpleCondition

sealed abstract class BinaryOperationExpr[T, U, Out, Self <: Expression[Out]](
    val a: Expression[T],
    val b: Expression[U],
    val functionName: String,
    isInfix: Boolean,
    create: (Expression[T], Expression[U]) => Self
) extends Expression[Out] {

  def operandA: Expression[T] = a
  def operandB: Expression[U] = b

  override val isNullable: Boolean = a.isNullable || b.isNullable

  override def fold[B](z: B)(f: (B, Expression[_]) => B): B = {
    val z1 = a.fold(f(z, this))(f)
    b.fold(z1)(f)
  }

  override def transform(f: PartialFunction[Expression[Out], Expression[Out]]): Expression[Out] = ???
  // f.applyOrElse(this, create(a.transform(f), b.transform(f)))

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
    extends BinaryOperationExpr[T, T, Boolean, EqExpr[T]](a, b, "=", isInfix = true, EqExpr.apply)
    with SimpleCondition

final case class NeqExpr[T](override val a: Expression[T], override val b: Expression[T])
    extends BinaryOperationExpr[T, T, Boolean, NeqExpr[T]](a, b, "<>", isInfix = true, NeqExpr.apply)
    with SimpleCondition

final case class LtExpr[T](override val a: Expression[T], override val b: Expression[T])(
    implicit val ordering: Ordering[T]
) extends BinaryOperationExpr[T, T, Boolean, LtExpr[T]](a, b, "<", isInfix = true, LtExpr.apply)
    with SimpleCondition

final case class GtExpr[T](override val a: Expression[T], override val b: Expression[T])(
    implicit val ordering: Ordering[T]
) extends BinaryOperationExpr[T, T, Boolean, GtExpr[T]](a, b, ">", isInfix = true, GtExpr.apply)
    with SimpleCondition

final case class LeExpr[T](override val a: Expression[T], override val b: Expression[T])(
    implicit val ordering: Ordering[T]
) extends BinaryOperationExpr[T, T, Boolean, LeExpr[T]](a, b, "<=", isInfix = true, LeExpr.apply)
    with SimpleCondition

final case class GeExpr[T](override val a: Expression[T], override val b: Expression[T])(
    implicit val ordering: Ordering[T]
) extends BinaryOperationExpr[T, T, Boolean, GeExpr[T]](a, b, ">=", isInfix = true, GeExpr.apply)
    with SimpleCondition

final case class PlusExpr[N](override val a: Expression[N], override val b: Expression[N])(
    implicit val numeric: Numeric[N]
) extends BinaryOperationExpr[N, N, N, PlusExpr[N]](a, b, "+", isInfix = true, PlusExpr.apply) {
  override val dataType: DataType.Aux[N] = a.dataType
}

final case class MinusExpr[N](override val a: Expression[N], override val b: Expression[N])(
    implicit val numeric: Numeric[N]
) extends BinaryOperationExpr[N, N, N, MinusExpr[N]](a, b, "-", isInfix = true, MinusExpr.apply) {
  override val dataType: DataType.Aux[N] = a.dataType
}

final case class TimesExpr[N](override val a: Expression[N], override val b: Expression[N])(
    implicit val numeric: Numeric[N]
) extends BinaryOperationExpr[N, N, N, TimesExpr[N]](a, b, "*", isInfix = true, TimesExpr.apply) {
  override val dataType: DataType.Aux[N] = a.dataType
}

final case class DivIntExpr[N](override val a: Expression[N], override val b: Expression[N])(
    implicit val integral: Integral[N]
) extends BinaryOperationExpr[N, N, N, DivIntExpr[N]](a, b, "/", isInfix = true, DivIntExpr.apply) {
  override val dataType: DataType.Aux[N] = a.dataType
}

final case class DivFracExpr[N](override val a: Expression[N], override val b: Expression[N])(
    implicit val fractional: Fractional[N]
) extends BinaryOperationExpr[N, N, N, DivFracExpr[N]](a, b, "/", isInfix = true, DivFracExpr.apply) {
  override val dataType: DataType.Aux[N] = a.dataType
}

final case class TimeMinusExpr(override val a: Expression[Time], override val b: Expression[Time])
    extends BinaryOperationExpr[Time, Time, Long, TimeMinusExpr](a, b, "-", isInfix = true, TimeMinusExpr.apply) {
  override val dataType: DataType.Aux[Long] = DataType[Long]
}

final case class TimeMinusPeriodExpr(override val a: Expression[Time], override val b: Expression[PeriodDuration])
    extends BinaryOperationExpr[Time, PeriodDuration, Time, TimeMinusPeriodExpr](
      a,
      b,
      "-",
      isInfix = true,
      TimeMinusPeriodExpr.apply
    ) {
  override val dataType: DataType.Aux[Time] = DataType[Time]
}

final case class TimePlusPeriodExpr(override val a: Expression[Time], override val b: Expression[PeriodDuration])
    extends BinaryOperationExpr[Time, PeriodDuration, Time, TimePlusPeriodExpr](
      a,
      b,
      "+",
      isInfix = true,
      TimePlusPeriodExpr
    ) {
  override val dataType: DataType.Aux[Time] = DataType[Time]
}

final case class PeriodPlusPeriodExpr(
    override val a: Expression[PeriodDuration],
    override val b: Expression[PeriodDuration]
) extends BinaryOperationExpr[PeriodDuration, PeriodDuration, PeriodDuration, PeriodPlusPeriodExpr](
      a,
      b,
      "+",
      isInfix = true,
      PeriodPlusPeriodExpr.apply
    ) {
  override val dataType: DataType.Aux[PeriodDuration] = DataType[PeriodDuration]
}

final case class ConcatExpr(override val a: Expression[String], override val b: Expression[String])
    extends BinaryOperationExpr[String, String, String, ConcatExpr](a, b, "+", isInfix = true, ConcatExpr.apply) {
  override val dataType: DataType.Aux[String] = DataType[String]
}

final case class ContainsExpr[T](override val a: Expression[Seq[T]], override val b: Expression[T])
    extends BinaryOperationExpr[Seq[T], T, Boolean, ContainsExpr[T]](
      a,
      b,
      "contains",
      isInfix = false,
      ContainsExpr.apply
    ) {

  override val dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

final case class ContainsAllExpr[T](override val a: Expression[Seq[T]], override val b: Expression[Seq[T]])
    extends BinaryOperationExpr[Seq[T], Seq[T], Boolean, ContainsAllExpr[T]](
      a,
      b,
      "containsAll",
      isInfix = false,
      ContainsAllExpr.apply
    ) {
  override val dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

final case class ContainsAnyExpr[T](override val a: Expression[Seq[T]], override val b: Expression[Seq[T]])
    extends BinaryOperationExpr[Seq[T], Seq[T], Boolean, ContainsAnyExpr[T]](
      a,
      b,
      "containsAny",
      isInfix = false,
      ContainsAnyExpr.apply
    ) {
  override val dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

final case class ContainsSameExpr[T](override val a: Expression[Seq[T]], override val b: Expression[Seq[T]])
    extends BinaryOperationExpr[Seq[T], Seq[T], Boolean, ContainsSameExpr[T]](
      a,
      b,
      "containsSame",
      isInfix = false,
      ContainsSameExpr.apply
    ) {
  override val dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

final case class TupleExpr[T, U](val e1: Expression[T], val e2: Expression[U])(
    implicit rtt: DataType.Aux[T],
    rtu: DataType.Aux[U]
) extends Expression[(T, U)] {
  override val dataType: DataType.Aux[(T, U)] = DataType[(T, U)]
  override val kind: ExprKind = ExprKind.combine(e1.kind, e2.kind)
  override val isNullable: Boolean = false

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = {
    val z1 = e1.fold(f(z, this))(f)
    e2.fold(z1)(f)
  }

  override def encode: String = s"(${e1.encode}, ${e2.encode})"
  override def toString: String = s"($e1, $e2)"
}

final case class ArrayExpr[T](exprs: Seq[Expression[T]])(implicit val elementDataType: DataType.Aux[T])
    extends Expression[Seq[T]] {
  override val dataType: DataType.Aux[Seq[T]] = DataType[Seq[T]]
  override val kind: ExprKind = exprs.foldLeft(Const: ExprKind)((a, e) => ExprKind.combine(a, e.kind))
  override val isNullable: Boolean = false

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = exprs.foldLeft(f(z, this))((a, e) => e.fold(a)(f))

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

  override def toString: String = s"IF ($condition) THEN $positive ELSE $negative"
  override def encode: String = s"if(${condition.encode},${positive.encode},${negative.encode}"
}

final case class InExpr[T](expr: Expression[T], values: Set[T])
    extends UnaryOperationExpr[T, Boolean, InExpr[T]](expr, "in", InExpr(_, values))
    with SimpleCondition {

  override def encode: String = values.toSeq.map(_.toString).sorted.mkString(s"in(${expr.encode}, (", ",", "))")
  override def toString: String =
    expr.toString + CollectionUtils.mkStringWithLimit(values, 10, " IN (", ", ", ")")
}

final case class NotInExpr[T](expr: Expression[T], values: Set[T])
    extends UnaryOperationExpr[T, Boolean, NotInExpr[T]](expr, "notIn", NotInExpr(_, values))
    with SimpleCondition {

  override def encode: String = values.toSeq.map(_.toString).sorted.mkString(s"notIn(${expr.encode}, (", ",", "))")
  override def toString: String =
    expr.toString + CollectionUtils.mkStringWithLimit(values, 10, " NOT IN (", ", ", ")")
}

final case class DimIdInExpr[T, R](dim: Dimension.Aux2[T, R], values: SortedSetIterator[R]) extends SimpleCondition {
  override val kind: ExprKind = Simple
  override val isNullable: Boolean = false

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = f(z, this)

  override def encode: String = s"idIn($dim, (Iterator))"
  override def toString: String = s"$dim ID IN (Iterator)"

  override def equals(that: Any): Boolean = false
}

final case class DimIdNotInExpr[T, R](dim: Dimension.Aux2[T, R], values: SortedSetIterator[R]) extends SimpleCondition {
  override val kind: ExprKind = Simple
  override val isNullable: Boolean = false

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = f(z, this)

  override def encode: String = s"idNotIn($dim, (Iterator))"
  override def toString: String = s"$dim ID NOT IN (Iterator)"

  override def equals(that: Any): Boolean = false
}

final case class AndExpr(conditions: Seq[Condition]) extends Expression[Boolean] {
  override val dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override val kind: ExprKind = conditions.foldLeft(Const: ExprKind)((k, c) => ExprKind.combine(k, c.kind))
  override val isNullable: Boolean = false

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = conditions.foldLeft(f(z, this))((a, e) => e.fold(a)(f))

  override def toString: String = conditions.mkString("(", " AND ", ")")
  override def encode: String = conditions.map(_.encode).sorted.mkString("and(", ",", ")")
}

final case class OrExpr(conditions: Seq[Condition]) extends Expression[Boolean] {
  override val dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override val kind: ExprKind = conditions.foldLeft(Const: ExprKind)((k, c) => ExprKind.combine(k, c.kind))
  override val isNullable: Boolean = false

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = conditions.foldLeft(f(z, this))((a, e) => e.fold(a)(f))

  override def toString: String = conditions.mkString("(", " OR ", ")")
  override def encode: String = conditions.map(_.encode).sorted.mkString("or(", ",", ")")
}

sealed abstract class TypeConvertExpr[T, U, Self <: Expression[U]](expr: Expression[T], create: Expression[T] => Self)(
    implicit dtt: DataType.Aux[T],
    dtu: DataType.Aux[U]
) extends UnaryOperationExpr[T, U, Self](
      expr,
      dtt.meta.sqlTypeName.toLowerCase + "2" + dtu.meta.sqlTypeName.toLowerCase,
      create
    ) {

  override val dataType: DataType.Aux[U] = dtu
}

final case class Double2BigDecimalExpr(expr: Expression[Double])
    extends TypeConvertExpr[Double, BigDecimal, Double2BigDecimalExpr](expr, Double2BigDecimalExpr)

final case class BigDecimal2DoubleExpr(expr: Expression[BigDecimal])
    extends TypeConvertExpr[BigDecimal, Double, BigDecimal2DoubleExpr](expr, BigDecimal2DoubleExpr)

final case class Long2BigDecimalExpr(expr: Expression[Long])
    extends TypeConvertExpr[Long, BigDecimal, Long2BigDecimalExpr](expr, Long2BigDecimalExpr)
final case class Long2DoubleExpr(expr: Expression[Long])
    extends TypeConvertExpr[Long, Double, Long2DoubleExpr](expr, Long2DoubleExpr)

final case class Int2LongExpr(expr: Expression[Int])
    extends TypeConvertExpr[Int, Long, Int2LongExpr](expr, Int2LongExpr)
final case class Int2BigDecimalExpr(expr: Expression[Int])
    extends TypeConvertExpr[Int, BigDecimal, Int2BigDecimalExpr](expr, Int2BigDecimalExpr)
final case class Int2DoubleExpr(expr: Expression[Int])
    extends TypeConvertExpr[Int, Double, Int2DoubleExpr](expr, Int2DoubleExpr)

final case class Short2IntExpr(expr: Expression[Short])
    extends TypeConvertExpr[Short, Int, Short2IntExpr](expr, Short2IntExpr)
final case class Short2LongExpr(expr: Expression[Short])
    extends TypeConvertExpr[Short, Long, Short2LongExpr](expr, Short2LongExpr)
final case class Short2BigDecimalExpr(expr: Expression[Short])
    extends TypeConvertExpr[Short, BigDecimal, Short2BigDecimalExpr](expr, Short2BigDecimalExpr)
final case class Short2DoubleExpr(expr: Expression[Short])
    extends TypeConvertExpr[Short, Double, Short2DoubleExpr](expr, Short2DoubleExpr)

final case class Byte2ShortExpr(expr: Expression[Byte])
    extends TypeConvertExpr[Byte, Short, Byte2ShortExpr](expr, Byte2ShortExpr)
final case class Byte2IntExpr(expr: Expression[Byte])
    extends TypeConvertExpr[Byte, Int, Byte2IntExpr](expr, Byte2IntExpr)
final case class Byte2LongExpr(expr: Expression[Byte])
    extends TypeConvertExpr[Byte, Long, Byte2LongExpr](expr, Byte2LongExpr)
final case class Byte2BigDecimalExpr(expr: Expression[Byte])
    extends TypeConvertExpr[Byte, BigDecimal, Byte2BigDecimalExpr](expr, Byte2BigDecimalExpr)
final case class Byte2DoubleExpr(expr: Expression[Byte])
    extends TypeConvertExpr[Byte, Double, Byte2DoubleExpr](expr, Byte2DoubleExpr)

final case class ToStringExpr[T](expr: Expression[T])(implicit dt: DataType.Aux[T])
    extends TypeConvertExpr[T, String, ToStringExpr[T]](expr, ToStringExpr(_))
