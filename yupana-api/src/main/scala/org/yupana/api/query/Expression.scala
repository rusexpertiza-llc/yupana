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

import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.schema.{ Dimension, ExternalLink, LinkField, Metric }
import org.yupana.api.types._
import org.yupana.api.utils.{ CollectionUtils, SortedSetIterator }

sealed trait Expression extends Serializable {
  type Out

  def dataType: DataType.Aux[Out]

  def kind: ExprKind

  def as(name: String): QueryField = QueryField(name, this)

  def encode: String

  def fold[O](z: O)(f: (O, Expression) => O): O

  def transform(pf: PartialFunction[Expression, Expression]): Expression.Aux[Out] = {
    pf.applyOrElse(this, identity[Expression]).asInstanceOf[Expression.Aux[Out]]
  }

  def aux: Expression.Aux[Out] = this.asInstanceOf[Expression.Aux[Out]]

  lazy val flatten: Set[Expression] = fold(Set.empty[Expression])(_ + _)

  private lazy val encoded = encode
  private lazy val encodedHashCode = encoded.hashCode()

  override def toString: String = encoded

  override def hashCode(): Int = encodedHashCode

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case that: Expression => this.encoded == that.encoded
      case _                => false
    }
  }
}

object Expression {
  type Aux[T] = Expression { type Out = T }

  type Condition = Expression.Aux[Boolean]
}

abstract class WindowFunctionExpr extends Expression {
  type In
  val operation: WindowOperation[In]
  val expr: Expression.Aux[In]

  override type Out = operation.Out
  override def dataType: DataType.Aux[Out] = operation.dataType
  override def kind: ExprKind = if (expr.kind == Simple || expr.kind == Const) Window else Invalid

  override def fold[O](z: O)(f: (O, Expression) => O): O = expr.fold(f(z, this))(f)
  override def transform(pf: PartialFunction[Expression, Expression]): Expression.Aux[Out] = {
    WindowFunctionExpr(operation, expr.transform(pf)).asInstanceOf[Expression.Aux[Out]]
  }

  override def encode: String = s"winFunc(${operation.name},${expr.encode})"
  override def toString: String = s"${operation.name}($expr)"
}

object WindowFunctionExpr {
  def apply[T](op: WindowOperation[T], e: Expression.Aux[T]): WindowFunctionExpr = new WindowFunctionExpr {
    override type In = T
    override val operation: WindowOperation[T] = op
    override val expr: Expression.Aux[T] = e
  }
  def unapply(arg: WindowFunctionExpr): Option[(WindowOperation[arg.In], Expression.Aux[arg.In])] = {
    Some((arg.operation, arg.expr))
  }
}

abstract class AggregateExpr extends Expression {
  type In
  val aggregation: Aggregation[In]
  val expr: Expression.Aux[In]

  override def kind: ExprKind = if (expr.kind == Simple || expr.kind == Const) Aggregate else Invalid

  override def fold[O](z: O)(f: (O, Expression) => O): O = expr.fold(f(z, this))(f)

  override def transform(pf: PartialFunction[Expression, Expression]): Expression.Aux[Out] = {
    AggregateExpr(aggregation, expr.transform(pf)).asInstanceOf[Expression.Aux[Out]]
  }

  override def encode: String = s"agg(${aggregation.name},${expr.encode})"
  override def toString: String = s"${aggregation.name}($expr)"
}

object AggregateExpr {
  def apply[T](a: Aggregation[T], e: Expression.Aux[T]): Expression.Aux[a.Out] = new AggregateExpr {
    override type In = T
    override type Out = a.Out
    override def dataType: DataType.Aux[Out] = a.dataType

    override val aggregation: Aggregation[T] = a
    override val expr: Expression.Aux[T] = e
  }

  def unapply(arg: AggregateExpr): Option[(Aggregation[arg.In], Expression.Aux[arg.In])] = {
    Some((arg.aggregation, arg.expr))
  }
}

abstract class ConstantExpr extends Expression {
  def v: Out
  override def encode: String = s"const($v:${v.getClass.getSimpleName})"
  override def kind: ExprKind = Const

  override def fold[O](z: O)(f: (O, Expression) => O): O = f(z, this)
}

case class PlaceholderExpr[T]()(implicit val dataType: DataType.Aux[T]) extends Expression {
  override type Out = T
  override def kind: ExprKind = Simple

  override def encode: String = s"?:${dataType.classTag.runtimeClass.getSimpleName}"
  override def fold[O](z: O)(f: (O, Expression) => O): O = f(z, this)
}

object ConstantExpr {
  type Aux[T] = ConstantExpr { type Out = T }

  def apply[T](value: T)(implicit rt: DataType.Aux[T]): ConstantExpr.Aux[T] = new ConstantExpr {
    override type Out = T
    override val v: T = value
    override def dataType: DataType.Aux[T] = rt
  }

  def unapply(c: ConstantExpr): Option[c.Out] = Some(c.v)
}

case object NullExpr extends Expression {
  override type Out = Null
  override val dataType: DataType.Aux[Null] = DataType[Null]
  override def kind: ExprKind = Const
  override def encode: String = "null"
  override def fold[O](z: O)(f: (O, Expression) => O): O = f(z, this)
}

case object TimeExpr extends Expression {
  override type Out = Time
  override val dataType: DataType.Aux[Time] = DataType[Time]
  override def kind: ExprKind = Simple

  override def fold[O](z: O)(f: (O, Expression) => O): O = f(z, this)

  override def encode: String = s"time()"
  def toField: QueryField = QueryField("time", this)
}

class DimensionExpr[T](val dimension: Dimension.Aux[T]) extends Expression {
  override type Out = T
  override val dataType: DataType.Aux[dimension.T] = dimension.dataType
  override def kind: ExprKind = Simple

  override def fold[O](z: O)(f: (O, Expression) => O): O = f(z, this)

  override def encode: String = s"dim(${dimension.name})"
  def toField: QueryField = QueryField(dimension.name, this)
}

object DimensionExpr {
  def apply[T](dimension: Dimension.Aux[T]): DimensionExpr[T] = new DimensionExpr(dimension)
  def unapply(expr: DimensionExpr[_]): Option[Dimension.Aux[expr.Out]] =
    Some(expr.dimension.asInstanceOf[Dimension.Aux[expr.Out]])
}

class DimensionIdExpr(val dimension: Dimension) extends Expression {
  override type Out = String
  override val dataType: DataType.Aux[String] = DataType[String]
  override def kind: ExprKind = Simple

  override def fold[O](z: O)(f: (O, Expression) => O): O = f(z, this)

  override def encode: String = s"dimId(${dimension.name})"
  def toField: QueryField = QueryField(dimension.name, this)
}

object DimensionIdExpr {
  def apply(dimension: Dimension): DimensionIdExpr = new DimensionIdExpr(dimension)
  def unapply(expr: DimensionIdExpr): Option[Dimension] = Some(expr.dimension)
}

case class MetricExpr[T](metric: Metric.Aux[T]) extends Expression {
  override type Out = T
  override def dataType: DataType.Aux[metric.T] = metric.dataType
  override def kind: ExprKind = Simple

  override def fold[O](z: O)(f: (O, Expression) => O): O = f(z, this)

  override def encode: String = s"metric(${metric.name})"
  def toField: QueryField = QueryField(metric.name, this)
}

case class LinkExpr[T](link: ExternalLink, linkField: LinkField.Aux[T]) extends Expression {
  override type Out = T
  override val dataType: DataType.Aux[linkField.T] = linkField.dataType
  override def kind: ExprKind = Simple

  override def fold[O](z: O)(f: (O, Expression) => O): O = f(z, this)

  override def encode: String = s"link(${link.linkName}, ${linkField.name})"
  def queryFieldName: String = link.linkName + "_" + linkField.name
  def toField: QueryField = QueryField(queryFieldName, this)
}

object LinkExpr {
//  def apply[T](link: ExternalLink, field: LinkField.Aux[T]): LinkExpr[T] = new LinkExpr(link, field)
  def apply(link: ExternalLink, field: String): LinkExpr[String] = new LinkExpr(link, LinkField[String](field))
//  def unapply(expr: LinkExpr[_]): Option[(ExternalLink, String)] = Some((expr.link, expr.linkField.name))
}

abstract class UnaryOperationExpr[T, U](
    expr: Expression.Aux[T],
    functionName: String
) extends Expression {
  override type Out = U
  override def dataType: DataType.Aux[U]

  type Self <: UnaryOperationExpr[T, U]
  def create(newExpr: Expression.Aux[T]): Self

  override def kind: ExprKind = expr.kind

  override def fold[O](z: O)(f: (O, Expression) => O): O = expr.fold(f(z, this))(f)

  override def transform(pf: PartialFunction[Expression, Expression]): Expression.Aux[U] = {
    if (pf.isDefinedAt(this)) {
      pf(this).asInstanceOf[Expression.Aux[Out]]
    } else {
      create(expr.transform(pf))
    }
  }

  override def encode: String = s"$functionName($expr)"
}

case class UnaryMinusExpr[N](expr: Expression.Aux[N])(implicit val num: Numeric[N], val dataType: DataType.Aux[N])
    extends UnaryOperationExpr[N, N](expr, "-") {
  override type Self = UnaryMinusExpr[N]
  override def create(newExpr: Expression.Aux[N]): UnaryMinusExpr[N] = UnaryMinusExpr(newExpr)
}

case class AbsExpr[N](expr: Expression.Aux[N])(implicit val num: Numeric[N], val dataType: DataType.Aux[N])
    extends UnaryOperationExpr[N, N](expr, "abs") {
  override type Self = AbsExpr[N]
  override def create(newExpr: Expression.Aux[N]): AbsExpr[N] = AbsExpr(newExpr)
}

case class NotExpr(expr: Expression.Aux[Boolean]) extends UnaryOperationExpr[Boolean, Boolean](expr, "not") {
  override type Self = NotExpr
  override def create(newExpr: Expression.Aux[Boolean]): NotExpr = NotExpr(newExpr)
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

case class LengthExpr(expr: Expression.Aux[String]) extends UnaryOperationExpr[String, Int](expr, "length") {
  override def dataType: DataType.Aux[Int] = DataType[Int]
  override type Self = LengthExpr
  override def create(newExpr: Expression.Aux[String]): LengthExpr = LengthExpr(newExpr)
}

case class LowerExpr(expr: Expression.Aux[String]) extends UnaryOperationExpr[String, String](expr, "lower") {
  override def dataType: DataType.Aux[String] = DataType[String]
  override type Self = LowerExpr
  override def create(newExpr: Expression.Aux[String]): LowerExpr = LowerExpr(newExpr)
}

case class UpperExpr(expr: Expression.Aux[String]) extends UnaryOperationExpr[String, String](expr, "upper") {
  override def dataType: DataType.Aux[String] = DataType[String]
  override type Self = UpperExpr
  override def create(newExpr: Expression.Aux[String]): UpperExpr = UpperExpr(newExpr)
}

case class TokensExpr(expr: Expression.Aux[String]) extends UnaryOperationExpr[String, Array[String]](expr, "tokens") {
  override def dataType: DataType.Aux[Array[String]] = DataType[Array[String]]
  override type Self = TokensExpr
  override def create(newExpr: Expression.Aux[String]): TokensExpr = TokensExpr(newExpr)
}

case class SplitExpr(expr: Expression.Aux[String]) extends UnaryOperationExpr[String, Array[String]](expr, "split") {
  override def dataType: DataType.Aux[Array[String]] = DataType[Array[String]]
  override type Self = SplitExpr
  override def create(newExpr: Expression.Aux[String]): SplitExpr = SplitExpr(newExpr)
}

case class ExtractYearExpr(expr: Expression.Aux[Time]) extends UnaryOperationExpr[Time, Int](expr, "extractYear") {
  override def dataType: DataType.Aux[Int] = DataType[Int]
  override type Self = ExtractYearExpr
  override def create(newExpr: Expression.Aux[Time]): ExtractYearExpr = ExtractYearExpr(newExpr)
}

case class ExtractMonthExpr(expr: Expression.Aux[Time]) extends UnaryOperationExpr[Time, Int](expr, "extractMonth") {
  override def dataType: DataType.Aux[Int] = DataType[Int]
  override type Self = ExtractMonthExpr
  override def create(newExpr: Expression.Aux[Time]): ExtractMonthExpr = ExtractMonthExpr(newExpr)
}

case class ExtractDayExpr(expr: Expression.Aux[Time]) extends UnaryOperationExpr[Time, Int](expr, "extractDay") {
  override def dataType: DataType.Aux[Int] = DataType[Int]
  override type Self = ExtractDayExpr
  override def create(newExpr: Expression.Aux[Time]): ExtractDayExpr = ExtractDayExpr(newExpr)
}

case class ExtractHourExpr(expr: Expression.Aux[Time]) extends UnaryOperationExpr[Time, Int](expr, "extractHour") {
  override def dataType: DataType.Aux[Int] = DataType[Int]
  override type Self = ExtractHourExpr
  override def create(newExpr: Expression.Aux[Time]): ExtractHourExpr = ExtractHourExpr(newExpr)
}

case class ExtractMinuteExpr(expr: Expression.Aux[Time]) extends UnaryOperationExpr[Time, Int](expr, "extractMinute") {
  override def dataType: DataType.Aux[Int] = DataType[Int]
  override type Self = ExtractMinuteExpr
  override def create(newExpr: Expression.Aux[Time]): ExtractMinuteExpr = ExtractMinuteExpr(newExpr)
}

case class ExtractSecondExpr(expr: Expression.Aux[Time]) extends UnaryOperationExpr[Time, Int](expr, "extractSecond") {
  override def dataType: DataType.Aux[Int] = DataType[Int]
  override type Self = ExtractSecondExpr
  override def create(newExpr: Expression.Aux[Time]): ExtractSecondExpr = ExtractSecondExpr(newExpr)
}

case class TrunkYearExpr(expr: Expression.Aux[Time]) extends UnaryOperationExpr[Time, Time](expr, "trunkYear") {
  override def dataType: DataType.Aux[Time] = DataType[Time]
  override type Self = TrunkYearExpr
  override def create(newExpr: Expression.Aux[Time]): TrunkYearExpr = TrunkYearExpr(newExpr)
}

case class TrunkMonthExpr(expr: Expression.Aux[Time]) extends UnaryOperationExpr[Time, Time](expr, "trunkMonth") {
  override def dataType: DataType.Aux[Time] = DataType[Time]
  override type Self = TrunkMonthExpr
  override def create(newExpr: Expression.Aux[Time]): TrunkMonthExpr = TrunkMonthExpr(newExpr)
}

case class TrunkWeekExpr(expr: Expression.Aux[Time]) extends UnaryOperationExpr[Time, Time](expr, "trunkWeek") {
  override def dataType: DataType.Aux[Time] = DataType[Time]
  override type Self = TrunkWeekExpr
  override def create(newExpr: Expression.Aux[Time]): TrunkWeekExpr = TrunkWeekExpr(newExpr)
}

case class TrunkDayExpr(expr: Expression.Aux[Time]) extends UnaryOperationExpr[Time, Time](expr, "trunkDay") {
  override def dataType: DataType.Aux[Time] = DataType[Time]
  override type Self = TrunkDayExpr
  override def create(newExpr: Expression.Aux[Time]): TrunkDayExpr = TrunkDayExpr(newExpr)
}

case class TrunkHourExpr(expr: Expression.Aux[Time]) extends UnaryOperationExpr[Time, Time](expr, "trunkHour") {
  override def dataType: DataType.Aux[Time] = DataType[Time]
  override type Self = TrunkHourExpr
  override def create(newExpr: Expression.Aux[Time]): TrunkHourExpr = TrunkHourExpr(newExpr)
}

case class TrunkMinuteExpr(expr: Expression.Aux[Time]) extends UnaryOperationExpr[Time, Time](expr, "trunkMinute") {
  override def dataType: DataType.Aux[Time] = DataType[Time]
  override type Self = TrunkMinuteExpr
  override def create(newExpr: Expression.Aux[Time]): TrunkMinuteExpr = TrunkMinuteExpr(newExpr)
}

case class TrunkSecondExpr(expr: Expression.Aux[Time]) extends UnaryOperationExpr[Time, Time](expr, "trunkSecond") {
  override def dataType: DataType.Aux[Time] = DataType[Time]
  override type Self = TrunkSecondExpr
  override def create(newExpr: Expression.Aux[Time]): TrunkSecondExpr = TrunkSecondExpr(newExpr)
}

case class IsNullExpr[T](expr: Expression.Aux[T]) extends UnaryOperationExpr[T, Boolean](expr, "isNull") {
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override type Self = IsNullExpr[T]
  override def create(newExpr: Expression.Aux[T]): IsNullExpr[T] = IsNullExpr(newExpr)
}

case class IsNotNullExpr[T](expr: Expression.Aux[T]) extends UnaryOperationExpr[T, Boolean](expr, "isNotNull") {
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override type Self = IsNotNullExpr[T]
  override def create(newExpr: Expression.Aux[T]): IsNotNullExpr[T] = IsNotNullExpr(newExpr)
}

case class TypeConvertExpr[T, U](tc: TypeConverter[T, U], expr: Expression.Aux[T]) extends Expression {
  override type Out = U
  override def dataType: DataType.Aux[U] = tc.dataType
  override def kind: ExprKind = expr.kind

  override def fold[O](z: O)(f: (O, Expression) => O): O = expr.fold(f(z, this))(f)
  override def transform(pf: PartialFunction[Expression, Expression]): Expression.Aux[U] = {
    if (pf.isDefinedAt(this)) {
      pf(this).asInstanceOf[Expression.Aux[Out]]
    } else {
      TypeConvertExpr(tc, expr.transform(pf))
    }
  }

  override def encode: String = s"${tc.functionName}($expr)"
}

abstract class BinaryOperationExpr[T, U, O](
    a: Expression.Aux[T],
    b: Expression.Aux[U],
    functionName: String,
    isInfix: Boolean
) extends Expression {

  override type Out = O

  type Self <: BinaryOperationExpr[T, U, O]
  def create(a: Expression.Aux[T], b: Expression.Aux[U]): Self

  override def fold[B](z: B)(f: (B, Expression) => B): B = {
    val z1 = a.fold(f(z, this))(f)
    b.fold(z1)(f)
  }

  override def transform(pf: PartialFunction[Expression, Expression]): Expression.Aux[O] = {
    if (pf.isDefinedAt(this)) {
      pf(this).asInstanceOf[Expression.Aux[Out]]
    } else {
      create(a.transform(pf), b.transform(pf))
    }
  }

  override def toString: String = if (isInfix) s"$a $functionName $b" else s"$functionName($a, $b)"
  override def encode: String = s"$functionName(${a.encode}, ${b.encode})"

  override def kind: ExprKind = ExprKind.combine(a.kind, b.kind)
}

case class EqExpr[T](a: Expression.Aux[T], b: Expression.Aux[T])
    extends BinaryOperationExpr[T, T, Boolean](a, b, "=", true) {
  override type Self = EqExpr[T]

  override def create(a: Expression.Aux[T], b: Expression.Aux[T]): EqExpr[T] = EqExpr(a, b)
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

case class NeqExpr[T](a: Expression.Aux[T], b: Expression.Aux[T])
    extends BinaryOperationExpr[T, T, Boolean](a, b, "<>", true) {
  override type Self = NeqExpr[T]

  override def create(a: Expression.Aux[T], b: Expression.Aux[T]): NeqExpr[T] = NeqExpr(a, b)
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

case class LtExpr[T: Ordering](a: Expression.Aux[T], b: Expression.Aux[T])
    extends BinaryOperationExpr[T, T, Boolean](a, b, "<", true) {
  override type Self = LtExpr[T]

  override def create(a: Expression.Aux[T], b: Expression.Aux[T]): LtExpr[T] = LtExpr(a, b)
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

case class GtExpr[T: Ordering](a: Expression.Aux[T], b: Expression.Aux[T])
    extends BinaryOperationExpr[T, T, Boolean](a, b, ">", true) {
  override type Self = GtExpr[T]

  override def create(a: Expression.Aux[T], b: Expression.Aux[T]): GtExpr[T] = GtExpr(a, b)
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

case class LeExpr[T: Ordering](a: Expression.Aux[T], b: Expression.Aux[T])
    extends BinaryOperationExpr[T, T, Boolean](a, b, "<=", true) {
  override type Self = LeExpr[T]

  override def create(a: Expression.Aux[T], b: Expression.Aux[T]): LeExpr[T] = LeExpr(a, b)
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

case class GeExpr[T: Ordering](a: Expression.Aux[T], b: Expression.Aux[T])
    extends BinaryOperationExpr[T, T, Boolean](a, b, ">=", true) {
  override type Self = GeExpr[T]

  override def create(a: Expression.Aux[T], b: Expression.Aux[T]): GeExpr[T] = GeExpr(a, b)
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

case class PlusExpr[N](a: Expression.Aux[N], b: Expression.Aux[N])(implicit val numeric: Numeric[N])
    extends BinaryOperationExpr[N, N, N](a, b, "+", true) {
  override type Self = PlusExpr[N]
  override def dataType: DataType.Aux[N] = a.dataType
  override def create(a: Expression.Aux[N], b: Expression.Aux[N]): PlusExpr[N] = PlusExpr(a, b)
}

case class MinusExpr[N: Numeric](a: Expression.Aux[N], b: Expression.Aux[N])
    extends BinaryOperationExpr[N, N, N](a, b, "-", true) {
  override type Self = MinusExpr[N]
  override def dataType: DataType.Aux[N] = a.dataType
  override def create(a: Expression.Aux[N], b: Expression.Aux[N]): MinusExpr[N] = MinusExpr(a, b)
}

case class TimesExpr[N: Numeric](a: Expression.Aux[N], b: Expression.Aux[N])
    extends BinaryOperationExpr[N, N, N](a, b, "*", true) {
  override type Self = TimesExpr[N]
  override def dataType: DataType.Aux[N] = a.dataType
  override def create(a: Expression.Aux[N], b: Expression.Aux[N]): TimesExpr[N] = TimesExpr(a, b)
}

case class DivIntExpr[N: Integral](a: Expression.Aux[N], b: Expression.Aux[N])
    extends BinaryOperationExpr[N, N, N](a, b, "/", true) {
  override type Self = DivIntExpr[N]
  override def dataType: DataType.Aux[N] = a.dataType
  override def create(a: Expression.Aux[N], b: Expression.Aux[N]): DivIntExpr[N] = DivIntExpr(a, b)
}

case class DivFracExpr[N: Fractional](a: Expression.Aux[N], b: Expression.Aux[N])
    extends BinaryOperationExpr[N, N, N](a, b, "/", true) {
  override type Self = DivFracExpr[N]
  override def dataType: DataType.Aux[N] = a.dataType
  override def create(a: Expression.Aux[N], b: Expression.Aux[N]): DivFracExpr[N] = DivFracExpr(a, b)
}

case class ContainsExpr[T](a: Expression.Aux[Array[T]], b: Expression.Aux[T])
    extends BinaryOperationExpr[Array[T], T, Boolean](a, b, "contains", false) {
  override type Self = ContainsExpr[T]

  override def create(a: Expression.Aux[Array[T]], b: Expression.Aux[T]): ContainsExpr[T] = ContainsExpr(a, b)
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

case class ContainsAllExpr[T](a: Expression.Aux[Array[T]], b: Expression.Aux[Array[T]])
    extends BinaryOperationExpr[Array[T], Array[T], Boolean](a, b, "containsAll", false) {
  override type Self = ContainsAllExpr[T]

  override def create(a: Expression.Aux[Array[T]], b: Expression.Aux[Array[T]]): ContainsAllExpr[T] =
    ContainsAllExpr(a, b)
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

case class ContainsAnyExpr[T](a: Expression.Aux[Array[T]], b: Expression.Aux[Array[T]])
    extends BinaryOperationExpr[Array[T], Array[T], Boolean](a, b, "containsAny", false) {
  override type Self = ContainsAnyExpr[T]

  override def create(a: Expression.Aux[Array[T]], b: Expression.Aux[Array[T]]): ContainsAnyExpr[T] =
    ContainsAnyExpr(a, b)
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

case class ContainsSameExpr[T](a: Expression.Aux[Array[T]], b: Expression.Aux[Array[T]])
    extends BinaryOperationExpr[Array[T], Array[T], Boolean](a, b, "containsSame", false) {
  override type Self = ContainsSameExpr[T]

  override def create(a: Expression.Aux[Array[T]], b: Expression.Aux[Array[T]]): ContainsSameExpr[T] =
    ContainsSameExpr(a, b)
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
}

case class TupleExpr[T, U](e1: Expression.Aux[T], e2: Expression.Aux[U])(
    implicit rtt: DataType.Aux[T],
    rtu: DataType.Aux[U]
) extends Expression {
  override type Out = (T, U)
  override def dataType: DataType.Aux[(T, U)] = DataType[(T, U)]
  override def kind: ExprKind = ExprKind.combine(e1.kind, e2.kind)

  override def fold[O](z: O)(f: (O, Expression) => O): O = {
    val z1 = e1.fold(f(z, this))(f)
    e2.fold(z1)(f)
  }

  override def transform(pf: PartialFunction[Expression, Expression]): Expression.Aux[(T, U)] = {
    if (pf.isDefinedAt(this)) {
      pf(this).asInstanceOf[Expression.Aux[Out]]
    } else {
      TupleExpr(e1.transform(pf), e2.transform(pf))
    }
  }

  override def encode: String = s"($e1, $e2)"
}

case class ArrayExpr[T](exprs: Array[Expression.Aux[T]])(implicit val elementDataType: DataType.Aux[T])
    extends Expression {
  override type Out = Array[T]
  override val dataType: DataType.Aux[Array[T]] = DataType[Array[T]]
  override def kind: ExprKind = exprs.foldLeft(Const: ExprKind)((a, e) => ExprKind.combine(a, e.kind))

  override def fold[O](z: O)(f: (O, Expression) => O): O = exprs.foldLeft(f(z, this))((a, e) => e.fold(a)(f))

  override def transform(pf: PartialFunction[Expression, Expression]): Expression.Aux[Array[T]] = {
    if (pf.isDefinedAt(this)) {
      pf(this).asInstanceOf[Expression.Aux[Out]]
    } else {
      ArrayExpr(exprs.map(_.transform(pf)))
    }
  }

  override def encode: String = exprs.mkString("[", ", ", "]")
  override def toString: String = CollectionUtils.mkStringWithLimit(exprs)
}

case class ConditionExpr[T](
    condition: Condition,
    positive: Expression.Aux[T],
    negative: Expression.Aux[T]
) extends Expression {
  override type Out = T
  override def dataType: DataType.Aux[T] = positive.dataType
  override def kind: ExprKind = ExprKind.combine(condition.kind, ExprKind.combine(positive.kind, negative.kind))

  override def fold[O](z: O)(f: (O, Expression) => O): O = {
    val z1 = condition.fold(f(z, this))(f)
    val z2 = positive.fold(z1)(f)
    negative.fold(z2)(f)
  }

  override def transform(pf: PartialFunction[Expression, Expression]): Expression.Aux[T] = {
    if (pf.isDefinedAt(this)) {
      pf(this).asInstanceOf[Expression.Aux[Out]]
    } else {
      ConditionExpr(condition.transform(pf), positive.transform(pf), negative.transform(pf))
    }
  }

  override def toString: String = s"IF ($condition) THEN $positive ELSE $negative"
  override def encode: String = s"if(${condition.encode},${positive.encode},${negative.encode}"
}

trait InExpr extends Expression {
  override type Out = Boolean
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]

  type T
  val expr: Expression.Aux[T]
  val values: Set[T]
  override def kind: ExprKind = expr.kind

  override def fold[O](z: O)(f: (O, Expression) => O): O = expr.fold(f(z, this))(f)
  override def transform(pf: PartialFunction[Expression, Expression]): Expression.Aux[Out] = {
    if (pf.isDefinedAt(this)) {
      pf(this).asInstanceOf[Condition]
    } else {
      InExpr(expr.transform(pf), values)
    }
  }

  override def encode: String = values.toSeq.map(_.toString).sorted.mkString(s"in(${expr.encode}, (", ",", "))")
  override def toString: String =
    expr.toString + CollectionUtils.mkStringWithLimit(values, 10, " IN (", ", ", ")")
}

object InExpr {
  def apply[T0](e: Expression.Aux[T0], vs: Set[T0]): Condition = new InExpr {
    override type T = T0
    override val expr: Expression.Aux[T] = e
    override val values: Set[T] = vs
  }

  def unapply(i: InExpr): Option[(Expression.Aux[i.T], Set[i.T])] = Some((i.expr, i.values))
}

case class NotInExpr[T](expr: Expression.Aux[T], values: Set[T]) extends Expression {
  override type Out = Boolean
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override def kind: ExprKind = expr.kind

  override def fold[O](z: O)(f: (O, Expression) => O): O = expr.fold(f(z, this))(f)
  override def transform(pf: PartialFunction[Expression, Expression]): Expression.Aux[Out] = {
    if (pf.isDefinedAt(this)) {
      pf(this).asInstanceOf[Condition]
    } else {
      NotInExpr(expr.transform(pf), values)
    }
  }

  override def encode: String = values.toSeq.map(_.toString).sorted.mkString(s"notIn(${expr.encode}, (", ",", "))")
  override def toString: String =
    expr.toString + CollectionUtils.mkStringWithLimit(values, 10, " NOT IN (", ", ", ")")
}

case class DimIdInExpr[T, R](dim: Dimension.Aux2[T, R], values: SortedSetIterator[R]) extends Expression {
  override type Out = Boolean
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override def kind: ExprKind = Simple

  override def fold[O](z: O)(f: (O, Expression) => O): O = f(z, this)

  override def encode: String = s"idIn($dim, (Iterator))"
  override def toString: String = s"$dim ID IN (Iterator)"
}

case class DimIdNotInExpr[T, R](dim: Dimension.Aux2[T, R], values: SortedSetIterator[R]) extends Expression {
  override type Out = Boolean
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override def kind: ExprKind = Simple

  override def fold[O](z: O)(f: (O, Expression) => O): O = f(z, this)

  override def encode: String = s"idNotIn($dim, (Iterator))"
  override def toString: String = s"$dim ID NOT IN (Iterator)"
}

case class AndExpr(conditions: Seq[Condition]) extends Expression {
  override type Out = Boolean
  override val dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override def kind: ExprKind = conditions.foldLeft(Const: ExprKind)((k, c) => ExprKind.combine(k, c.kind))

  override def fold[O](z: O)(f: (O, Expression) => O): O = conditions.foldLeft(f(z, this))((a, e) => e.fold(a)(f))

  override def transform(pf: PartialFunction[Expression, Expression]): Expression.Aux[Boolean] = {
    if (pf.isDefinedAt(this)) {
      pf(this).asInstanceOf[Expression.Aux[Boolean]]
    } else {
      AndExpr(conditions.map(_.transform(pf)))
    }
  }

  override def toString: String = conditions.mkString("(", " AND ", ")")
  override def encode: String = conditions.map(_.encode).sorted.mkString("and(", ",", ")")
}

case class OrExpr(conditions: Seq[Condition]) extends Expression {
  override type Out = Boolean
  override val dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override def kind: ExprKind = conditions.foldLeft(Const: ExprKind)((k, c) => ExprKind.combine(k, c.kind))

  override def fold[O](z: O)(f: (O, Expression) => O): O = conditions.foldLeft(f(z, this))((a, e) => e.fold(a)(f))

  override def transform(pf: PartialFunction[Expression, Expression]): Expression.Aux[Boolean] = {
    if (pf.isDefinedAt(this)) {
      pf(this).asInstanceOf[Expression.Aux[Boolean]]
    } else {
      OrExpr(conditions.map(_.transform(pf)))
    }
  }

  override def toString: String = conditions.mkString("(", " OR ", ")")
  override def encode: String = conditions.map(_.encode).sorted.mkString("or(", ",", ")")
}
