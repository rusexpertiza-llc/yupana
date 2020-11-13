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

sealed trait Expression[Out] extends Serializable {
  def dataType: DataType.Aux[Out]

  def kind: ExprKind

  def as(name: String): QueryField = QueryField(name, this)

  def encode: String

  def fold[O](z: O)(f: (O, Expression[_]) => O): O

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

sealed abstract class WindowFunctionExpr[In, Out](val expr: Expression[In], name: String)
    extends UnaryOperationExpr[In, Out](expr, name) {
  override def kind: ExprKind = if (expr.kind == Simple || expr.kind == Const) Window else Invalid
  override def encode: String = s"winFunc($name,${expr.encode})"
}

final case class LagExpr[I](override val expr: Expression[I]) extends WindowFunctionExpr[I, I](expr, "lag") {
  override def dataType: DataType.Aux[I] = expr.dataType
}

sealed abstract class AggregateExpr[In, M, Out](val expr: Expression[In], name: String)
    extends UnaryOperationExpr[In, Out](expr, name) {

  override def kind: ExprKind = if (expr.kind == Simple || expr.kind == Const) Aggregate else Invalid
  override def encode: String = s"agg($name,${expr.encode})"
}

final case class MinExpr[I](override val expr: Expression[I])(implicit val ord: Ordering[I])
    extends AggregateExpr[I, I, I](expr, "min") {
  override def dataType: DataType.Aux[I] = expr.dataType
}

final case class MaxExpr[I](override val expr: Expression[I])(implicit val ord: Ordering[I])
    extends AggregateExpr[I, I, I](expr, "max") {
  override def dataType: DataType.Aux[I] = expr.dataType
}

final case class SumExpr[I](override val expr: Expression[I])(implicit val numeric: Numeric[I])
    extends AggregateExpr[I, I, I](expr, "sum") {
  override def dataType: DataType.Aux[I] = expr.dataType
}

final case class CountExpr[I](override val expr: Expression[I]) extends AggregateExpr[I, Long, Long](expr, "count") {
  override def dataType: DataType.Aux[Long] = DataType[Long]
}

final case class DistinctCountExpr[I](override val expr: Expression[I])
    extends AggregateExpr[I, Set[I], Int](expr, "distinct_count") {
  override def dataType: DataType.Aux[Int] = DataType[Int]
}

final case class DistinctRandomExpr[I](override val expr: Expression[I])
    extends AggregateExpr[I, Set[I], I](expr, "distinct_random") {
  override def dataType: DataType.Aux[I] = expr.dataType
}

final case class ConstantExpr[T](v: T)(implicit dt: DataType.Aux[T]) extends Expression[T] {
  override val dataType: DataType.Aux[T] = dt
  override def encode: String = {
    if (dataType.isArray) {
      val adt = dataType.asInstanceOf[ArrayDataType[T]]
      val vStr = v.asInstanceOf[adt.T].map(_.toString.replaceAll(",", "\\\\,")).mkString(",")
      s"const([$vStr]:${v.getClass.getSimpleName})"
    } else {
      s"const($v:${v.getClass.getSimpleName})"
    }
  }
  override def kind: ExprKind = Const

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

final case class MetricExpr[T](metric: Metric.Aux[T]) extends Expression[T] {
  override def dataType: DataType.Aux[metric.T] = metric.dataType
  override def kind: ExprKind = Simple

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = f(z, this)

  override def encode: String = s"metric(${metric.name})"
  def toField: QueryField = QueryField(metric.name, this)
}

final case class LinkExpr[T](link: ExternalLink, linkField: LinkField.Aux[T]) extends Expression[T] {
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

sealed abstract class UnaryOperationExpr[In, Out](expr: Expression[In], functionName: String) extends Expression[Out] {
  override def dataType: DataType.Aux[Out]
  override def kind: ExprKind = expr.kind

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = expr.fold(f(z, this))(f)

  override def encode: String = s"$functionName($expr)"
  override def toString: String = s"$functionName($expr)"
}

final case class UnaryMinusExpr[N](expr: Expression[N])(implicit val num: Numeric[N])
    extends UnaryOperationExpr[N, N](expr, "-") {
  override def dataType: DataType.Aux[N] = expr.dataType
}

final case class AbsExpr[N](expr: Expression[N])(implicit val num: Numeric[N])
    extends UnaryOperationExpr[N, N](expr, "abs") {
  override def dataType: DataType.Aux[N] = expr.dataType
}

final case class NotExpr(expr: Expression[Boolean]) extends UnaryOperationExpr[Boolean, Boolean](expr, "not") {
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

final case class LengthExpr(expr: Expression[String]) extends UnaryOperationExpr[String, Int](expr, "length") {
  override def dataType: DataType.Aux[Int] = DataType[Int]
}

final case class LowerExpr(expr: Expression[String]) extends UnaryOperationExpr[String, String](expr, "lower") {
  override def dataType: DataType.Aux[String] = DataType[String]
}

final case class UpperExpr(expr: Expression[String]) extends UnaryOperationExpr[String, String](expr, "upper") {
  override def dataType: DataType.Aux[String] = DataType[String]
}

final case class TokensExpr(expr: Expression[String]) extends UnaryOperationExpr[String, Seq[String]](expr, "tokens") {
  override def dataType: DataType.Aux[Seq[String]] = DataType[Seq[String]]
}

final case class ArrayTokensExpr(expr: Expression[Seq[String]])
    extends UnaryOperationExpr[Seq[String], Seq[String]](expr, "tokens") {
  override def dataType: DataType.Aux[Seq[String]] = DataType[Seq[String]]
}

final case class SplitExpr(expr: Expression[String]) extends UnaryOperationExpr[String, Seq[String]](expr, "split") {
  override def dataType: DataType.Aux[Seq[String]] = DataType[Seq[String]]
}

final case class ArrayToStringExpr[T](expr: Expression[Seq[T]])
    extends UnaryOperationExpr[Seq[T], String](expr, "array_to_string") {
  override def dataType: DataType.Aux[String] = DataType[String]
}

final case class ArrayLengthExpr[T](expr: Expression[Seq[T]]) extends UnaryOperationExpr[Seq[T], Int](expr, "length") {
  override def dataType: DataType.Aux[Int] = DataType[Int]
}

final case class ExtractYearExpr(expr: Expression[Time]) extends UnaryOperationExpr[Time, Int](expr, "extractYear") {
  override def dataType: DataType.Aux[Int] = DataType[Int]
}

final case class ExtractMonthExpr(expr: Expression[Time]) extends UnaryOperationExpr[Time, Int](expr, "extractMonth") {
  override def dataType: DataType.Aux[Int] = DataType[Int]
}

final case class ExtractDayExpr(expr: Expression[Time]) extends UnaryOperationExpr[Time, Int](expr, "extractDay") {
  override def dataType: DataType.Aux[Int] = DataType[Int]
}

final case class ExtractHourExpr(expr: Expression[Time]) extends UnaryOperationExpr[Time, Int](expr, "extractHour") {
  override def dataType: DataType.Aux[Int] = DataType[Int]
}

final case class ExtractMinuteExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Int](expr, "extractMinute") {
  override def dataType: DataType.Aux[Int] = DataType[Int]
}

final case class ExtractSecondExpr(expr: Expression[Time])
    extends UnaryOperationExpr[Time, Int](expr, "extractSecond") {
  override def dataType: DataType.Aux[Int] = DataType[Int]
}

final case class TruncYearExpr(expr: Expression[Time]) extends UnaryOperationExpr[Time, Time](expr, "truncYear") {
  override def dataType: DataType.Aux[Time] = DataType[Time]
}

final case class TruncMonthExpr(expr: Expression[Time]) extends UnaryOperationExpr[Time, Time](expr, "truncMonth") {
  override def dataType: DataType.Aux[Time] = DataType[Time]
}

final case class TruncWeekExpr(expr: Expression[Time]) extends UnaryOperationExpr[Time, Time](expr, "truncWeek") {
  override def dataType: DataType.Aux[Time] = DataType[Time]
}

final case class TruncDayExpr(expr: Expression[Time]) extends UnaryOperationExpr[Time, Time](expr, "truncDay") {
  override def dataType: DataType.Aux[Time] = DataType[Time]
}

final case class TruncHourExpr(expr: Expression[Time]) extends UnaryOperationExpr[Time, Time](expr, "truncHour") {
  override def dataType: DataType.Aux[Time] = DataType[Time]
}

final case class TruncMinuteExpr(expr: Expression[Time]) extends UnaryOperationExpr[Time, Time](expr, "truncMinute") {
  override def dataType: DataType.Aux[Time] = DataType[Time]
}

final case class TruncSecondExpr(expr: Expression[Time]) extends UnaryOperationExpr[Time, Time](expr, "truncSecond") {
  override def dataType: DataType.Aux[Time] = DataType[Time]
}

final case class IsNullExpr[T](expr: Expression[T]) extends UnaryOperationExpr[T, Boolean](expr, "isNull") {
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

final case class IsNotNullExpr[T](expr: Expression[T]) extends UnaryOperationExpr[T, Boolean](expr, "isNotNull") {
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

final case class TypeConvertExpr[T, U](tc: TypeConverter[T, U], expr: Expression[T]) extends Expression[U] {
  override def dataType: DataType.Aux[U] = tc.dataType
  override def kind: ExprKind = expr.kind

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = expr.fold(f(z, this))(f)

  override def encode: String = s"${tc.functionName}($expr)"
}

sealed abstract class BinaryOperationExpr[T, U, Out](
    val a: Expression[T],
    val b: Expression[U],
    val functionName: String,
    isInfix: Boolean
) extends Expression[Out] {

  override def fold[B](z: B)(f: (B, Expression[_]) => B): B = {
    val z1 = a.fold(f(z, this))(f)
    b.fold(z1)(f)
  }
  override def toString: String = if (isInfix) s"$a $functionName $b" else s"$functionName($a, $b)"
  override def encode: String = s"$functionName(${a.encode}, ${b.encode})"

  override def kind: ExprKind = ExprKind.combine(a.kind, b.kind)
}

final case class EqExpr[T](override val a: Expression[T], override val b: Expression[T])
    extends BinaryOperationExpr[T, T, Boolean](a, b, "=", true) {
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

final case class NeqExpr[T](override val a: Expression[T], override val b: Expression[T])
    extends BinaryOperationExpr[T, T, Boolean](a, b, "<>", true) {
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

final case class LtExpr[T](override val a: Expression[T], override val b: Expression[T])(
    implicit val ordering: Ordering[T]
) extends BinaryOperationExpr[T, T, Boolean](a, b, "<", true) {
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

final case class GtExpr[T](override val a: Expression[T], override val b: Expression[T])(
    implicit val ordering: Ordering[T]
) extends BinaryOperationExpr[T, T, Boolean](a, b, ">", true) {
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

final case class LeExpr[T](override val a: Expression[T], override val b: Expression[T])(
    implicit val ordering: Ordering[T]
) extends BinaryOperationExpr[T, T, Boolean](a, b, "<=", true) {
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

final case class GeExpr[T](override val a: Expression[T], override val b: Expression[T])(
    implicit val ordering: Ordering[T]
) extends BinaryOperationExpr[T, T, Boolean](a, b, ">=", true) {
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

final case class PlusExpr[N](override val a: Expression[N], override val b: Expression[N])(
    implicit val numeric: Numeric[N]
) extends BinaryOperationExpr[N, N, N](a, b, "+", true) {
  override def dataType: DataType.Aux[N] = a.dataType
}

final case class MinusExpr[N](override val a: Expression[N], override val b: Expression[N])(
    implicit val numeric: Numeric[N]
) extends BinaryOperationExpr[N, N, N](a, b, "-", true) {
  override def dataType: DataType.Aux[N] = a.dataType
}

final case class TimesExpr[N](override val a: Expression[N], override val b: Expression[N])(
    implicit val numeric: Numeric[N]
) extends BinaryOperationExpr[N, N, N](a, b, "*", true) {
  override def dataType: DataType.Aux[N] = a.dataType
}

final case class DivIntExpr[N](override val a: Expression[N], override val b: Expression[N])(
    implicit val integral: Integral[N]
) extends BinaryOperationExpr[N, N, N](a, b, "/", true) {
  override def dataType: DataType.Aux[N] = a.dataType
}

final case class DivFracExpr[N](override val a: Expression[N], override val b: Expression[N])(
    implicit val fractional: Fractional[N]
) extends BinaryOperationExpr[N, N, N](a, b, "/", true) {
  override def dataType: DataType.Aux[N] = a.dataType
}

final case class TimeMinusExpr(override val a: Expression[Time], override val b: Expression[Time])
    extends BinaryOperationExpr[Time, Time, Long](a, b, "-", true) {
  override val dataType: DataType.Aux[Long] = DataType[Long]
}

final case class TimeMinusPeriodExpr(override val a: Expression[Time], override val b: Expression[Period])
    extends BinaryOperationExpr[Time, Period, Time](a, b, "-", true) {
  override val dataType: DataType.Aux[Time] = DataType[Time]
}

final case class TimePlusPeriodExpr(override val a: Expression[Time], override val b: Expression[Period])
    extends BinaryOperationExpr[Time, Period, Time](a, b, "+", true) {
  override val dataType: DataType.Aux[Time] = DataType[Time]
}

final case class PeriodPlusPeriodExpr(override val a: Expression[Period], override val b: Expression[Period])
    extends BinaryOperationExpr[Period, Period, Period](a, b, "+", true) {
  override val dataType: DataType.Aux[Period] = DataType[Period]
}

final case class ConcatExpr(override val a: Expression[String], override val b: Expression[String])
    extends BinaryOperationExpr[String, String, String](a, b, "+", true) {
  override val dataType: DataType.Aux[String] = DataType[String]
}

final case class ContainsExpr[T](override val a: Expression[Seq[T]], override val b: Expression[T])
    extends BinaryOperationExpr[Seq[T], T, Boolean](a, b, "contains", false) {

  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

final case class ContainsAllExpr[T](override val a: Expression[Seq[T]], override val b: Expression[Seq[T]])
    extends BinaryOperationExpr[Seq[T], Seq[T], Boolean](a, b, "containsAll", false) {
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

final case class ContainsAnyExpr[T](override val a: Expression[Seq[T]], override val b: Expression[Seq[T]])
    extends BinaryOperationExpr[Seq[T], Seq[T], Boolean](a, b, "containsAny", false) {
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

final case class ContainsSameExpr[T](override val a: Expression[Seq[T]], override val b: Expression[Seq[T]])
    extends BinaryOperationExpr[Seq[T], Seq[T], Boolean](a, b, "containsSame", false) {
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

final case class TupleExpr[T, U](e1: Expression[T], e2: Expression[U])(
    implicit rtt: DataType.Aux[T],
    rtu: DataType.Aux[U]
) extends Expression[(T, U)] {
  override def dataType: DataType.Aux[(T, U)] = DataType[(T, U)]
  override def kind: ExprKind = ExprKind.combine(e1.kind, e2.kind)

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = {
    val z1 = e1.fold(f(z, this))(f)
    e2.fold(z1)(f)
  }

  override def encode: String = s"($e1, $e2)"
}

final case class ArrayExpr[T](exprs: Seq[Expression[T]])(implicit val elementDataType: DataType.Aux[T])
    extends Expression[Seq[T]] {
  override val dataType: DataType.Aux[Seq[T]] = DataType[Seq[T]]
  override def kind: ExprKind = exprs.foldLeft(Const: ExprKind)((a, e) => ExprKind.combine(a, e.kind))

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = exprs.foldLeft(f(z, this))((a, e) => e.fold(a)(f))

  override def encode: String = exprs.mkString("[", ", ", "]")
  override def toString: String = CollectionUtils.mkStringWithLimit(exprs)
}

final case class ConditionExpr[T](
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

  override def toString: String = s"IF ($condition) THEN $positive ELSE $negative"
  override def encode: String = s"if(${condition.encode},${positive.encode},${negative.encode}"
}

final case class InExpr[T](expr: Expression[T], values: Set[T]) extends Expression[Boolean] {
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]

  override def kind: ExprKind = expr.kind

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = expr.fold(f(z, this))(f)

  override def encode: String = values.toSeq.map(_.toString).sorted.mkString(s"in(${expr.encode}, (", ",", "))")
  override def toString: String =
    expr.toString + CollectionUtils.mkStringWithLimit(values, 10, " IN (", ", ", ")")
}

final case class NotInExpr[T](expr: Expression[T], values: Set[T]) extends Expression[Boolean] {
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override def kind: ExprKind = expr.kind

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = expr.fold(f(z, this))(f)

  override def encode: String = values.toSeq.map(_.toString).sorted.mkString(s"notIn(${expr.encode}, (", ",", "))")
  override def toString: String =
    expr.toString + CollectionUtils.mkStringWithLimit(values, 10, " NOT IN (", ", ", ")")
}

final case class DimIdInExpr[T, R](dim: Dimension.Aux2[T, R], values: SortedSetIterator[R])
    extends Expression[Boolean] {
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override def kind: ExprKind = Simple

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = f(z, this)

  override def encode: String = s"idIn($dim, (Iterator))"
  override def toString: String = s"$dim ID IN (Iterator)"
}

final case class DimIdNotInExpr[T, R](dim: Dimension.Aux2[T, R], values: SortedSetIterator[R])
    extends Expression[Boolean] {
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override def kind: ExprKind = Simple

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = f(z, this)

  override def encode: String = s"idNotIn($dim, (Iterator))"
  override def toString: String = s"$dim ID NOT IN (Iterator)"
}

final case class AndExpr(conditions: Seq[Condition]) extends Expression[Boolean] {
  override val dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override def kind: ExprKind = conditions.foldLeft(Const: ExprKind)((k, c) => ExprKind.combine(k, c.kind))

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = conditions.foldLeft(f(z, this))((a, e) => e.fold(a)(f))

  override def toString: String = conditions.mkString("(", " AND ", ")")
  override def encode: String = conditions.map(_.encode).sorted.mkString("and(", ",", ")")
}

final case class OrExpr(conditions: Seq[Condition]) extends Expression[Boolean] {
  override val dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override def kind: ExprKind = conditions.foldLeft(Const: ExprKind)((k, c) => ExprKind.combine(k, c.kind))

  override def fold[O](z: O)(f: (O, Expression[_]) => O): O = conditions.foldLeft(f(z, this))((a, e) => e.fold(a)(f))

  override def toString: String = conditions.mkString("(", " OR ", ")")
  override def encode: String = conditions.map(_.encode).sorted.mkString("or(", ",", ")")
}
