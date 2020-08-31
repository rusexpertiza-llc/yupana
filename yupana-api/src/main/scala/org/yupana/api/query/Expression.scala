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

import org.joda.time.Period
import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.schema.{ Dimension, ExternalLink, LinkField, Metric }
import org.yupana.api.types._
import org.yupana.api.utils.{ CollectionUtils, SortedSetIterator }

trait Transformer {
  def apply[T](e: Expression[T]): Option[Expression[T]]
  def isDefinedAt[T](e: Expression[T]): Boolean = apply(e).nonEmpty
  def applyIfPossible[T](e: Expression[T]): Expression[T] = apply(e) getOrElse e
}

sealed trait Expression[Out] extends Serializable {
  def dataType: DataType.Aux[Out]

  def kind: ExprKind

  def as(name: String): QueryField = QueryField(name, this)

  def encode: String

  def fold[O](z: O)(f: (O, Expression[_]) => O): O

  def transform(t: Transformer): Expression[Out] = {
    t.applyIfPossible(this)
  }

  def aux: Expression[Out] = this.asInstanceOf[Expression[Out]]

  lazy val flatten: Set[Expression[_]] = fold(Set.empty[Expression[_]])(_ + _)

  private lazy val encoded = encode
  private lazy val encodedHashCode = encoded.hashCode()

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

sealed abstract class WindowFunctionExpr[T, U](val expr: Expression[T], name: String)
    extends UnaryOperationExpr[T, U](expr, name) {
  type In = T
  override def kind: ExprKind = if (expr.kind == Simple || expr.kind == Const) Window else Invalid
  override def encode: String = s"winFunc($name,${expr.encode})"
}

case class LagExpr[I](override val expr: Expression[I]) extends WindowFunctionExpr[I, I](expr, "lag") {
  override def dataType: DataType.Aux[I] = expr.dataType
  override type Self = LagExpr[I]

  override def create(newExpr: Expression[I]): LagExpr[I] = LagExpr(newExpr)
}

sealed abstract class AggregateExpr[T, U](val expr: Expression[T], name: String)
    extends UnaryOperationExpr[T, U](expr, name) {
  type In = T
  type Interim
  override def aux: AggregateExpr.Aux[In, Interim, U] = this.asInstanceOf[AggregateExpr.Aux[In, Interim, U]]

  override def kind: ExprKind = if (expr.kind == Simple || expr.kind == Const) Aggregate else Invalid
  override def encode: String = s"agg($name,${expr.encode})"
}

object AggregateExpr {
  type Aux[I, M, O] = AggregateExpr[I, O] { type Interim = M }
}

case class MinExpr[I](override val expr: Expression[I])(implicit val ord: Ordering[I])
    extends AggregateExpr[I, I](expr, "min") {
  override type Interim = I
  override def dataType: DataType.Aux[I] = expr.dataType
  override type Self = MinExpr[I]
  override def create(newExpr: Expression[I]): MinExpr[I] = MinExpr(newExpr)
}

case class MaxExpr[I](override val expr: Expression[I])(implicit val ord: Ordering[I])
    extends AggregateExpr[I, I](expr, "max") {
  override type Interim = I
  override def dataType: DataType.Aux[I] = expr.dataType
  override type Self = MaxExpr[I]
  override def create(newExpr: Expression[I]): MaxExpr[I] = MaxExpr(newExpr)
}

case class SumExpr[I](override val expr: Expression[I])(implicit val numeric: Numeric[I])
    extends AggregateExpr[I, I](expr, "sum") {
  override type Interim = I
  override def dataType: DataType.Aux[I] = expr.dataType
  override type Self = SumExpr[I]
  override def create(newExpr: Expression[I]): SumExpr[I] = SumExpr(newExpr)
}

case class CountExpr[I](override val expr: Expression[I]) extends AggregateExpr[I, Long](expr, "count") {
  override type Interim = Long
  override def dataType: DataType.Aux[Long] = DataType[Long]
  override type Self = CountExpr[I]
  override def create(newExpr: Expression[I]): CountExpr[I] = CountExpr(newExpr)
}

case class DistinctCountExpr[I](override val expr: Expression[I])
    extends AggregateExpr[I, Int](expr, "distinct_count") {
  override type Interim = Set[I]
  override def dataType: DataType.Aux[Int] = DataType[Int]
  override type Self = DistinctCountExpr[I]
  override def create(newExpr: Expression[I]): DistinctCountExpr[I] = DistinctCountExpr(newExpr)
}

case class DistinctRandomExpr[I](override val expr: Expression[I]) extends AggregateExpr[I, I](expr, "distinct_count") {
  override type Interim = Set[I]
  override def dataType: DataType.Aux[I] = expr.dataType
  override type Self = DistinctRandomExpr[I]
  override def create(newExpr: Expression[I]): DistinctRandomExpr[I] = DistinctRandomExpr(newExpr)
}

case class ConstantExpr[T](v: T)(implicit dt: DataType.Aux[T]) extends Expression[T] {
  override def dataType: DataType.Aux[T] = dt
  override def encode: String = s"const($v:${v.getClass.getSimpleName})"
  override def kind: ExprKind = Const

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = f(z, this)
}

case class NullExpr[T](override val dataType: DataType.Aux[T]) extends Expression[T] {
  override def kind: ExprKind = Const
  override def encode: String = s"null:${dataType.classTag.runtimeClass.getSimpleName}"
  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = f(z, this)
}

case object TimeExpr extends Expression[Time] {
  override val dataType: DataType.Aux[Time] = DataType[Time]
  override def kind: ExprKind = Simple

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = f(z, this)

  override def encode: String = s"time()"
  def toField: QueryField = QueryField("time", this)
}

case class DimensionExpr[T](dimension: Dimension.Aux[T]) extends Expression[T] {
  override val dataType: DataType.Aux[dimension.T] = dimension.dataType
  override def kind: ExprKind = Simple

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = f(z, this)

  override def encode: String = s"dim(${dimension.name})"
  def toField: QueryField = QueryField(dimension.name, this)
}

case class DimensionIdExpr(dimension: Dimension) extends Expression[String] {
  override val dataType: DataType.Aux[String] = DataType[String]
  override def kind: ExprKind = Simple

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = f(z, this)

  override def encode: String = s"dimId(${dimension.name})"
  def toField: QueryField = QueryField(dimension.name, this)
}

//object DimensionIdExpr {
//  def apply(dimension: Dimension): DimensionIdExpr = new DimensionIdExpr(dimension)
//  def unapply(expr: DimensionIdExpr): Option[Dimension] = Some(expr.dimension)
//}

case class MetricExpr[T](metric: Metric.Aux[T]) extends Expression[T] {
  override def dataType: DataType.Aux[metric.T] = metric.dataType
  override def kind: ExprKind = Simple

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = f(z, this)

  override def encode: String = s"metric(${metric.name})"
  def toField: QueryField = QueryField(metric.name, this)
}

case class LinkExpr[T](link: ExternalLink, linkField: LinkField.Aux[T]) extends Expression[T] {
  override val dataType: DataType.Aux[linkField.T] = linkField.dataType
  override def kind: ExprKind = Simple

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = f(z, this)

  override def encode: String = s"link(${link.linkName}, ${linkField.name})"
  def queryFieldName: String = link.linkName + "_" + linkField.name
  def toField: QueryField = QueryField(queryFieldName, this)
}

object LinkExpr {
  def apply(link: ExternalLink, field: String): LinkExpr[String] = new LinkExpr(link, LinkField[String](field))
}

abstract class UnaryOperationExpr[T, U](
    expr: Expression[T],
    functionName: String
) extends Expression[U] {
  override def dataType: DataType.Aux[U]

  type Self <: UnaryOperationExpr[T, U]
  def create(newExpr: Expression[T]): Self

  override def kind: ExprKind = expr.kind

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = expr.fold(f(z, this))(f)

  override def transform(t: Transformer): Expression[U] = {
    t(this) getOrElse create(expr.transform(t))
  }

  override def encode: String = s"$functionName($expr)"
  override def toString: String = s"$functionName($expr)"
}

case class UnaryMinusExpr[N](expr: Expression[N])(implicit val num: Numeric[N])
    extends UnaryOperationExpr[N, N](expr, "-") {
  override type Self = UnaryMinusExpr[N]
  override def dataType: DataType.Aux[N] = expr.dataType
  override def create(newExpr: Expression[N]): UnaryMinusExpr[N] = UnaryMinusExpr(newExpr)
}

case class AbsExpr[N](expr: Expression[N])(implicit val num: Numeric[N]) extends UnaryOperationExpr[N, N](expr, "abs") {
  override type Self = AbsExpr[N]
  override def dataType: DataType.Aux[N] = expr.dataType
  override def create(newExpr: Expression[N]): AbsExpr[N] = AbsExpr(newExpr)
}

case class NotExpr(expr: Expression[Boolean]) extends UnaryOperationExpr[Boolean, Boolean](expr, "not") {
  override type Self = NotExpr
  override def create(newExpr: Expression[Boolean]): NotExpr = NotExpr(newExpr)
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

case class LengthExpr(expr: Expression[String]) extends UnaryOperationExpr[String, Int](expr, "length") {
  override def dataType: DataType.Aux[Int] = DataType[Int]
  override type Self = LengthExpr
  override def create(newExpr: Expression[String]): LengthExpr = LengthExpr(newExpr)
}

case class LowerExpr(expr: Expression[String]) extends UnaryOperationExpr[String, String](expr, "lower") {
  override def dataType: DataType.Aux[String] = DataType[String]
  override type Self = LowerExpr
  override def create(newExpr: Expression[String]): LowerExpr = LowerExpr(newExpr)
}

case class UpperExpr(expr: Expression[String]) extends UnaryOperationExpr[String, String](expr, "upper") {
  override def dataType: DataType.Aux[String] = DataType[String]
  override type Self = UpperExpr
  override def create(newExpr: Expression[String]): UpperExpr = UpperExpr(newExpr)
}

case class TokensExpr(expr: Expression[String]) extends UnaryOperationExpr[String, Array[String]](expr, "tokens") {
  override def dataType: DataType.Aux[Array[String]] = DataType[Array[String]]
  override type Self = TokensExpr
  override def create(newExpr: Expression[String]): TokensExpr = TokensExpr(newExpr)
}

case class ArrayTokensExpr(expr: Expression[Array[String]])
    extends UnaryOperationExpr[Array[String], Array[String]](expr, "tokens") {
  override def dataType: DataType.Aux[Array[String]] = DataType[Array[String]]
  override type Self = ArrayTokensExpr
  override def create(newExpr: Expression[Array[String]]): ArrayTokensExpr = ArrayTokensExpr(newExpr)
}

case class SplitExpr(expr: Expression[String]) extends UnaryOperationExpr[String, Array[String]](expr, "split") {
  override def dataType: DataType.Aux[Array[String]] = DataType[Array[String]]
  override type Self = SplitExpr
  override def create(newExpr: Expression[String]): SplitExpr = SplitExpr(newExpr)
}

case class ArrayToStringExpr[T](expr: Expression[Array[T]])
    extends UnaryOperationExpr[Array[T], String](expr, "array_to_string") {
  override def dataType: DataType.Aux[String] = DataType[String]
  override type Self = ArrayToStringExpr[T]
  override def create(newExpr: Expression[Array[T]]): ArrayToStringExpr[T] = ArrayToStringExpr(newExpr)
}

case class ArrayLengthExpr[T](expr: Expression[Array[T]]) extends UnaryOperationExpr[Array[T], Int](expr, "length") {
  override def dataType: DataType.Aux[Int] = DataType[Int]
  override type Self = ArrayLengthExpr[T]
  override def create(newExpr: Expression[Array[T]]): ArrayLengthExpr[T] = ArrayLengthExpr(newExpr)
}

case class ExtractYearExpr(expr: Expression[Time]) extends UnaryOperationExpr[Time, Int](expr, "extractYear") {
  override def dataType: DataType.Aux[Int] = DataType[Int]
  override type Self = ExtractYearExpr
  override def create(newExpr: Expression[Time]): ExtractYearExpr = ExtractYearExpr(newExpr)
}

case class ExtractMonthExpr(expr: Expression[Time]) extends UnaryOperationExpr[Time, Int](expr, "extractMonth") {
  override def dataType: DataType.Aux[Int] = DataType[Int]
  override type Self = ExtractMonthExpr
  override def create(newExpr: Expression[Time]): ExtractMonthExpr = ExtractMonthExpr(newExpr)
}

case class ExtractDayExpr(expr: Expression[Time]) extends UnaryOperationExpr[Time, Int](expr, "extractDay") {
  override def dataType: DataType.Aux[Int] = DataType[Int]
  override type Self = ExtractDayExpr
  override def create(newExpr: Expression[Time]): ExtractDayExpr = ExtractDayExpr(newExpr)
}

case class ExtractHourExpr(expr: Expression[Time]) extends UnaryOperationExpr[Time, Int](expr, "extractHour") {
  override def dataType: DataType.Aux[Int] = DataType[Int]
  override type Self = ExtractHourExpr
  override def create(newExpr: Expression[Time]): ExtractHourExpr = ExtractHourExpr(newExpr)
}

case class ExtractMinuteExpr(expr: Expression[Time]) extends UnaryOperationExpr[Time, Int](expr, "extractMinute") {
  override def dataType: DataType.Aux[Int] = DataType[Int]
  override type Self = ExtractMinuteExpr
  override def create(newExpr: Expression[Time]): ExtractMinuteExpr = ExtractMinuteExpr(newExpr)
}

case class ExtractSecondExpr(expr: Expression[Time]) extends UnaryOperationExpr[Time, Int](expr, "extractSecond") {
  override def dataType: DataType.Aux[Int] = DataType[Int]
  override type Self = ExtractSecondExpr
  override def create(newExpr: Expression[Time]): ExtractSecondExpr = ExtractSecondExpr(newExpr)
}

case class TruncYearExpr(expr: Expression[Time]) extends UnaryOperationExpr[Time, Time](expr, "truncYear") {
  override def dataType: DataType.Aux[Time] = DataType[Time]
  override type Self = TruncYearExpr
  override def create(newExpr: Expression[Time]): TruncYearExpr = TruncYearExpr(newExpr)
}

case class TruncMonthExpr(expr: Expression[Time]) extends UnaryOperationExpr[Time, Time](expr, "truncMonth") {
  override def dataType: DataType.Aux[Time] = DataType[Time]
  override type Self = TruncMonthExpr
  override def create(newExpr: Expression[Time]): TruncMonthExpr = TruncMonthExpr(newExpr)
}

case class TruncWeekExpr(expr: Expression[Time]) extends UnaryOperationExpr[Time, Time](expr, "truncWeek") {
  override def dataType: DataType.Aux[Time] = DataType[Time]
  override type Self = TruncWeekExpr
  override def create(newExpr: Expression[Time]): TruncWeekExpr = TruncWeekExpr(newExpr)
}

case class TruncDayExpr(expr: Expression[Time]) extends UnaryOperationExpr[Time, Time](expr, "truncDay") {
  override def dataType: DataType.Aux[Time] = DataType[Time]
  override type Self = TruncDayExpr
  override def create(newExpr: Expression[Time]): TruncDayExpr = TruncDayExpr(newExpr)
}

case class TruncHourExpr(expr: Expression[Time]) extends UnaryOperationExpr[Time, Time](expr, "truncHour") {
  override def dataType: DataType.Aux[Time] = DataType[Time]
  override type Self = TruncHourExpr
  override def create(newExpr: Expression[Time]): TruncHourExpr = TruncHourExpr(newExpr)
}

case class TruncMinuteExpr(expr: Expression[Time]) extends UnaryOperationExpr[Time, Time](expr, "truncMinute") {
  override def dataType: DataType.Aux[Time] = DataType[Time]
  override type Self = TruncMinuteExpr
  override def create(newExpr: Expression[Time]): TruncMinuteExpr = TruncMinuteExpr(newExpr)
}

case class TruncSecondExpr(expr: Expression[Time]) extends UnaryOperationExpr[Time, Time](expr, "truncSecond") {
  override def dataType: DataType.Aux[Time] = DataType[Time]
  override type Self = TruncSecondExpr
  override def create(newExpr: Expression[Time]): TruncSecondExpr = TruncSecondExpr(newExpr)
}

case class IsNullExpr[T](expr: Expression[T]) extends UnaryOperationExpr[T, Boolean](expr, "isNull") {
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override type Self = IsNullExpr[T]
  override def create(newExpr: Expression[T]): IsNullExpr[T] = IsNullExpr(newExpr)
}

case class IsNotNullExpr[T](expr: Expression[T]) extends UnaryOperationExpr[T, Boolean](expr, "isNotNull") {
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override type Self = IsNotNullExpr[T]
  override def create(newExpr: Expression[T]): IsNotNullExpr[T] = IsNotNullExpr(newExpr)
}

case class TypeConvertExpr[T, U](tc: TypeConverter[T, U], expr: Expression[T]) extends Expression[U] {
  override def dataType: DataType.Aux[U] = tc.dataType
  override def kind: ExprKind = expr.kind

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = expr.fold(f(z, this))(f)
  override def transform(t: Transformer): Expression[U] = {
    t(this) getOrElse TypeConvertExpr(tc, expr.transform(t))
  }

  override def encode: String = s"${tc.functionName}($expr)"
}

abstract class BinaryOperationExpr[T, U, O](
    a: Expression[T],
    b: Expression[U],
    functionName: String,
    isInfix: Boolean
) extends Expression[O] {

  type Self <: BinaryOperationExpr[T, U, O]
  def create(a: Expression[T], b: Expression[U]): Self

  override def fold[B](z: B)(f: (B, Expression[_]) => B): B = {
    val z1 = a.fold(f(z, this))(f)
    b.fold(z1)(f)
  }

  override def transform(t: Transformer): Expression[O] = {
    t(this) getOrElse create(a.transform(t), b.transform(t))
  }

  override def toString: String = if (isInfix) s"$a $functionName $b" else s"$functionName($a, $b)"
  override def encode: String = s"$functionName(${a.encode}, ${b.encode})"

  override def kind: ExprKind = ExprKind.combine(a.kind, b.kind)
}

case class EqExpr[T](a: Expression[T], b: Expression[T]) extends BinaryOperationExpr[T, T, Boolean](a, b, "=", true) {
  override type Self = EqExpr[T]

  override def create(a: Expression[T], b: Expression[T]): EqExpr[T] = EqExpr(a, b)
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

case class NeqExpr[T](a: Expression[T], b: Expression[T]) extends BinaryOperationExpr[T, T, Boolean](a, b, "<>", true) {
  override type Self = NeqExpr[T]

  override def create(a: Expression[T], b: Expression[T]): NeqExpr[T] = NeqExpr(a, b)
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

case class LtExpr[T](a: Expression[T], b: Expression[T])(implicit val ordering: Ordering[T])
    extends BinaryOperationExpr[T, T, Boolean](a, b, "<", true) {
  override type Self = LtExpr[T]

  override def create(a: Expression[T], b: Expression[T]): LtExpr[T] = LtExpr(a, b)
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

case class GtExpr[T](a: Expression[T], b: Expression[T])(implicit val ordering: Ordering[T])
    extends BinaryOperationExpr[T, T, Boolean](a, b, ">", true) {
  override type Self = GtExpr[T]

  override def create(a: Expression[T], b: Expression[T]): GtExpr[T] = GtExpr(a, b)
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

case class LeExpr[T](a: Expression[T], b: Expression[T])(implicit val ordering: Ordering[T])
    extends BinaryOperationExpr[T, T, Boolean](a, b, "<=", true) {
  override type Self = LeExpr[T]

  override def create(a: Expression[T], b: Expression[T]): LeExpr[T] = LeExpr(a, b)
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

case class GeExpr[T](a: Expression[T], b: Expression[T])(implicit val ordering: Ordering[T])
    extends BinaryOperationExpr[T, T, Boolean](a, b, ">=", true) {
  override type Self = GeExpr[T]

  override def create(a: Expression[T], b: Expression[T]): GeExpr[T] = GeExpr(a, b)
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

case class PlusExpr[N](a: Expression[N], b: Expression[N])(implicit val numeric: Numeric[N])
    extends BinaryOperationExpr[N, N, N](a, b, "+", true) {
  override type Self = PlusExpr[N]
  override def dataType: DataType.Aux[N] = a.dataType
  override def create(a: Expression[N], b: Expression[N]): PlusExpr[N] = PlusExpr(a, b)
}

case class MinusExpr[N](a: Expression[N], b: Expression[N])(implicit val numeric: Numeric[N])
    extends BinaryOperationExpr[N, N, N](a, b, "-", true) {
  override type Self = MinusExpr[N]
  override def dataType: DataType.Aux[N] = a.dataType
  override def create(a: Expression[N], b: Expression[N]): MinusExpr[N] = MinusExpr(a, b)
}

case class TimesExpr[N](a: Expression[N], b: Expression[N])(implicit val numeric: Numeric[N])
    extends BinaryOperationExpr[N, N, N](a, b, "*", true) {
  override type Self = TimesExpr[N]
  override def dataType: DataType.Aux[N] = a.dataType
  override def create(a: Expression[N], b: Expression[N]): TimesExpr[N] = TimesExpr(a, b)
}

case class DivIntExpr[N](a: Expression[N], b: Expression[N])(implicit val integral: Integral[N])
    extends BinaryOperationExpr[N, N, N](a, b, "/", true) {
  override type Self = DivIntExpr[N]
  override def dataType: DataType.Aux[N] = a.dataType
  override def create(a: Expression[N], b: Expression[N]): DivIntExpr[N] = DivIntExpr(a, b)
}

case class DivFracExpr[N](a: Expression[N], b: Expression[N])(implicit val fractional: Fractional[N])
    extends BinaryOperationExpr[N, N, N](a, b, "/", true) {
  override type Self = DivFracExpr[N]
  override def dataType: DataType.Aux[N] = a.dataType
  override def create(a: Expression[N], b: Expression[N]): DivFracExpr[N] = DivFracExpr(a, b)
}

case class TimeMinusExpr(a: Expression[Time], b: Expression[Time])
    extends BinaryOperationExpr[Time, Time, Long](a, b, "-", true) {
  override type Self = TimeMinusExpr
  override val dataType: DataType.Aux[Long] = DataType[Long]
  override def create(a: Expression[Time], b: Expression[Time]): TimeMinusExpr = TimeMinusExpr(a, b)
}

case class TimeMinusPeriodExpr(a: Expression[Time], b: Expression[Period])
    extends BinaryOperationExpr[Time, Period, Time](a, b, "-", true) {
  override type Self = TimeMinusPeriodExpr
  override val dataType: DataType.Aux[Time] = DataType[Time]
  override def create(a: Expression[Time], b: Expression[Period]): TimeMinusPeriodExpr =
    TimeMinusPeriodExpr(a, b)
}

case class TimePlusPeriodExpr(a: Expression[Time], b: Expression[Period])
    extends BinaryOperationExpr[Time, Period, Time](a, b, "+", true) {
  override type Self = TimePlusPeriodExpr
  override val dataType: DataType.Aux[Time] = DataType[Time]
  override def create(a: Expression[Time], b: Expression[Period]): TimePlusPeriodExpr =
    TimePlusPeriodExpr(a, b)
}

case class PeriodPlusPeriodExpr(a: Expression[Period], b: Expression[Period])
    extends BinaryOperationExpr[Period, Period, Period](a, b, "+", true) {
  override type Self = PeriodPlusPeriodExpr
  override val dataType: DataType.Aux[Period] = DataType[Period]
  override def create(a: Expression[Period], b: Expression[Period]): PeriodPlusPeriodExpr =
    PeriodPlusPeriodExpr(a, b)
}

case class ConcatExpr(a: Expression[String], b: Expression[String])
    extends BinaryOperationExpr[String, String, String](a, b, "+", true) {
  override type Self = ConcatExpr
  override val dataType: DataType.Aux[String] = DataType[String]
  override def create(a: Expression[String], b: Expression[String]): ConcatExpr = ConcatExpr(a, b)
}

case class ContainsExpr[T](a: Expression[Array[T]], b: Expression[T])
    extends BinaryOperationExpr[Array[T], T, Boolean](a, b, "contains", false) {
  type In = Array[T]
  type Item = T
  override type Self = ContainsExpr[T]

  override def create(a: Expression[Array[T]], b: Expression[T]): ContainsExpr[T] = ContainsExpr(a, b)
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

case class ContainsAllExpr[T](a: Expression[Array[T]], b: Expression[Array[T]])
    extends BinaryOperationExpr[Array[T], Array[T], Boolean](a, b, "containsAll", false) {
  override type Self = ContainsAllExpr[T]

  override def create(a: Expression[Array[T]], b: Expression[Array[T]]): ContainsAllExpr[T] =
    ContainsAllExpr(a, b)
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

case class ContainsAnyExpr[T](a: Expression[Array[T]], b: Expression[Array[T]])
    extends BinaryOperationExpr[Array[T], Array[T], Boolean](a, b, "containsAny", false) {
  override type Self = ContainsAnyExpr[T]

  override def create(a: Expression[Array[T]], b: Expression[Array[T]]): ContainsAnyExpr[T] =
    ContainsAnyExpr(a, b)
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

case class ContainsSameExpr[T](a: Expression[Array[T]], b: Expression[Array[T]])
    extends BinaryOperationExpr[Array[T], Array[T], Boolean](a, b, "containsSame", false) {
  override type Self = ContainsSameExpr[T]

  override def create(a: Expression[Array[T]], b: Expression[Array[T]]): ContainsSameExpr[T] =
    ContainsSameExpr(a, b)
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

case class TupleExpr[T, U](e1: Expression[T], e2: Expression[U])(
    implicit rtt: DataType.Aux[T],
    rtu: DataType.Aux[U]
) extends Expression[(T, U)] {
  override def dataType: DataType.Aux[(T, U)] = DataType[(T, U)]
  override def kind: ExprKind = ExprKind.combine(e1.kind, e2.kind)

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = {
    val z1 = e1.fold(f(z, this))(f)
    e2.fold(z1)(f)
  }

  override def transform(t: Transformer): Expression[(T, U)] = {
    t(this) getOrElse TupleExpr(e1.transform(t), e2.transform(t))
  }

  override def encode: String = s"($e1, $e2)"
}

case class ArrayExpr[T](exprs: Array[Expression[T]])(implicit val elementDataType: DataType.Aux[T])
    extends Expression[Array[T]] {
  override val dataType: DataType.Aux[Array[T]] = DataType[Array[T]]
  override def kind: ExprKind = exprs.foldLeft(Const: ExprKind)((a, e) => ExprKind.combine(a, e.kind))

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = exprs.foldLeft(f(z, this))((a, e) => e.fold(a)(f))

  override def transform(t: Transformer): Expression[Array[T]] = {
    t(this) getOrElse ArrayExpr(exprs.map(_.transform(t)))
  }

  override def encode: String = exprs.mkString("[", ", ", "]")
  override def toString: String = CollectionUtils.mkStringWithLimit(exprs)
}

case class ConditionExpr[T](
    condition: Condition,
    positive: Expression[T],
    negative: Expression[T]
) extends Expression[T] {
  override def dataType: DataType.Aux[T] = positive.dataType
  override def kind: ExprKind = ExprKind.combine(condition.kind, ExprKind.combine(positive.kind, negative.kind))

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = {
    val z1 = condition.fold(f(z, this))(f)
    val z2 = positive.fold(z1)(f)
    negative.fold(z2)(f)
  }

  override def transform(t: Transformer): Expression[T] = {
    t(this) getOrElse ConditionExpr(condition.transform(t), positive.transform(t), negative.transform(t))
  }

  override def toString: String = s"IF ($condition) THEN $positive ELSE $negative"
  override def encode: String = s"if(${condition.encode},${positive.encode},${negative.encode}"
}

case class InExpr[T](expr: Expression[T], values: Set[T]) extends Expression[Boolean] {
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]

  override def kind: ExprKind = expr.kind

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = expr.fold(f(z, this))(f)
  override def transform(t: Transformer): Expression[Boolean] = {
    t(this) getOrElse InExpr(expr.transform(t), values)
  }

  override def encode: String = values.toSeq.map(_.toString).sorted.mkString(s"in(${expr.encode}, (", ",", "))")
  override def toString: String =
    expr.toString + CollectionUtils.mkStringWithLimit(values, 10, " IN (", ", ", ")")
}

case class NotInExpr[T](expr: Expression[T], values: Set[T]) extends Expression[Boolean] {
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override def kind: ExprKind = expr.kind

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = expr.fold(f(z, this))(f)
  override def transform(t: Transformer): Expression[Boolean] = {
    t(this) getOrElse NotInExpr(expr.transform(t), values)

  }

  override def encode: String = values.toSeq.map(_.toString).sorted.mkString(s"notIn(${expr.encode}, (", ",", "))")
  override def toString: String =
    expr.toString + CollectionUtils.mkStringWithLimit(values, 10, " NOT IN (", ", ", ")")
}

case class DimIdInExpr[T, R](dim: Dimension.Aux2[T, R], values: SortedSetIterator[R]) extends Expression[Boolean] {
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override def kind: ExprKind = Simple

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = f(z, this)

  override def encode: String = s"idIn($dim, (Iterator))"
  override def toString: String = s"$dim ID IN (Iterator)"
}

case class DimIdNotInExpr[T, R](dim: Dimension.Aux2[T, R], values: SortedSetIterator[R]) extends Expression[Boolean] {
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override def kind: ExprKind = Simple

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = f(z, this)

  override def encode: String = s"idNotIn($dim, (Iterator))"
  override def toString: String = s"$dim ID NOT IN (Iterator)"
}

case class AndExpr(conditions: Seq[Condition]) extends Expression[Boolean] {
  override val dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override def kind: ExprKind = conditions.foldLeft(Const: ExprKind)((k, c) => ExprKind.combine(k, c.kind))

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = conditions.foldLeft(f(z, this))((a, e) => e.fold(a)(f))

  override def transform(t: Transformer): Expression[Boolean] = {
    t(this) getOrElse AndExpr(conditions.map(_.transform(t)))
  }

  override def toString: String = conditions.mkString("(", " AND ", ")")
  override def encode: String = conditions.map(_.encode).sorted.mkString("and(", ",", ")")
}

case class OrExpr(conditions: Seq[Condition]) extends Expression[Boolean] {
  override val dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override def kind: ExprKind = conditions.foldLeft(Const: ExprKind)((k, c) => ExprKind.combine(k, c.kind))

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = conditions.foldLeft(f(z, this))((a, e) => e.fold(a)(f))

  override def transform(t: Transformer): Expression[Boolean] = {
    t(this) getOrElse OrExpr(conditions.map(_.transform(t)))
  }

  override def toString: String = conditions.mkString("(", " OR ", ")")
  override def encode: String = conditions.map(_.encode).sorted.mkString("or(", ",", ")")
}
