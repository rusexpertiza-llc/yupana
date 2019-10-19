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
import org.yupana.api.schema.{ Dimension, ExternalLink, Metric }
import org.yupana.api.types._
import org.yupana.api.utils.{ CollectionUtils, SortedSetIterator }

sealed trait Expression extends Serializable {
  type Out

  def dataType: DataType.Aux[Out]

  def kind: ExprKind

  def as(name: String) = QueryField(name, this)

  def encode: String

  def fold[O](z: O)(f: (O, Expression) => O): O

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

sealed trait WindowFunctionExpr extends Expression {
  type In
  val operation: WindowOperation[In]
  val expr: Expression.Aux[In]

  override type Out = operation.Out
  override def dataType: DataType.Aux[Out] = operation.dataType
  override def kind: ExprKind = if (expr.kind == Simple || expr.kind == Const) Window else Invalid

  override def fold[O](z: O)(f: (O, Expression) => O): O = expr.fold(f(z, this))(f)

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

sealed trait AggregateExpr extends Expression {
  type In
  val aggregation: Aggregation[In]
  val expr: Expression.Aux[In]

  override def kind: ExprKind = if (expr.kind == Simple || expr.kind == Const) Aggregate else Invalid

  override def fold[O](z: O)(f: (O, Expression) => O): O = expr.fold(f(z, this))(f)

  override def encode: String = s"agg(${aggregation.name},${expr.encode})"
  override def toString: String = s"${aggregation.name}($expr)"
}

object AggregateExpr {
  type Aux[T] = AggregateExpr { type Out = T }

  def apply[T](a: Aggregation[T], e: Expression.Aux[T]): AggregateExpr.Aux[a.Out] = new AggregateExpr {
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

sealed trait ConstantExpr extends Expression {
  def v: Out
  override def encode: String = s"const($v)"
  override def kind: ExprKind = Const

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

case object TimeExpr extends Expression {
  override type Out = Time
  override val dataType: DataType.Aux[Time] = DataType[Time]
  override def kind: ExprKind = Simple

  override def fold[O](z: O)(f: (O, Expression) => O): O = f(z, this)

  override def encode: String = s"time()"
  def toField = QueryField("time", this)
}

class DimensionExpr(val dimension: Dimension) extends Expression {
  override type Out = String
  override val dataType: DataType.Aux[String] = DataType[String]
  override def kind: ExprKind = Simple

  override def fold[O](z: O)(f: (O, Expression) => O): O = f(z, this)

  override def encode: String = s"dim(${dimension.name})"
  def toField = QueryField(dimension.name, this)
}

object DimensionExpr {
  def apply(dimension: Dimension): DimensionExpr = new DimensionExpr(dimension)
  def unapply(expr: DimensionExpr): Option[Dimension] = Some(expr.dimension)
}

case class MetricExpr[T](metric: Metric.Aux[T]) extends Expression {
  override type Out = T
  override def dataType: DataType.Aux[metric.T] = metric.dataType
  override def kind: ExprKind = Simple

  override def fold[O](z: O)(f: (O, Expression) => O): O = f(z, this)

  override def encode: String = s"metric(${metric.name})"
  def toField = QueryField(metric.name, this)
}

class LinkExpr(val link: ExternalLink, val linkField: String) extends Expression {
  override type Out = String
  override val dataType: DataType.Aux[String] = DataType[String]
  override def kind: ExprKind = Simple

  override def fold[O](z: O)(f: (O, Expression) => O): O = f(z, this)

  override def encode: String = s"link(${link.linkName}, $linkField)"
  def queryFieldName: String = link.linkName + "_" + linkField
  def toField = QueryField(queryFieldName, this)
}

object LinkExpr {
  def apply(link: ExternalLink, field: String): Expression.Aux[String] = new LinkExpr(link, field)
  def unapply(expr: LinkExpr): Option[(ExternalLink, String)] = Some((expr.link, expr.linkField))
}

case class UnaryOperationExpr[T, U](function: UnaryOperation.Aux[T, U], expr: Expression.Aux[T]) extends Expression {
  override type Out = U
  override def dataType: DataType.Aux[U] = function.dataType
  override def kind: ExprKind = expr.kind

  override def fold[O](z: O)(f: (O, Expression) => O): O = expr.fold(f(z, this))(f)

  override def encode: String = s"${function.name}($expr)"
}

case class TypeConvertExpr[T, U](tc: TypeConverter[T, U], expr: Expression.Aux[T]) extends Expression {
  override type Out = U
  override def dataType: DataType.Aux[U] = tc.dataType
  override def kind: ExprKind = expr.kind

  override def fold[O](z: O)(f: (O, Expression) => O): O = expr.fold(f(z, this))(f)

  override def encode: String = s"${tc.functionName}($expr)"
}

case class BinaryOperationExpr[T, U, O](
    function: BinaryOperation.Aux[T, U, O],
    a: Expression.Aux[T],
    b: Expression.Aux[U]
) extends Expression {
  override type Out = O
  override def dataType: DataType.Aux[Out] = function.dataType

  override def fold[B](z: B)(f: (B, Expression) => B): B = {
    val z1 = a.fold(f(z, this))(f)
    b.fold(z1)(f)
  }

  override def toString: String = if (function.infix) s"$a $function $b" else s"$function($a, $b)"
  override def encode: String = s"$function(${a.encode}, ${b.encode})"

  override def kind: ExprKind = ExprKind.combine(a.kind, b.kind)
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

  override def encode: String = s"($e1, $e2)"
}

case class ArrayExpr[T](exprs: Array[Expression.Aux[T]])(implicit val elementDataType: DataType.Aux[T])
    extends Expression {
  override type Out = Array[T]
  override val dataType: DataType.Aux[Array[T]] = DataType[Array[T]]
  override def kind: ExprKind = exprs.foldLeft(Const: ExprKind)((a, e) => ExprKind.combine(a, e.kind))

  override def fold[O](z: O)(f: (O, Expression) => O): O = exprs.foldLeft(f(z, this))((a, e) => e.fold(a)(f))

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
  override def kind: ExprKind = ExprKind.combine(positive.kind, negative.kind)

  override def fold[O](z: O)(f: (O, Expression) => O): O = {
    val z1 = condition.fold(f(z, this))(f)
    val z2 = positive.fold(z1)(f)
    negative.fold(z2)(f)
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

  override def encode: String = values.toSeq.map(_.toString).sorted.mkString(s"notIn(${expr.encode}, (", ",", "))")
  override def toString: String =
    expr.toString + CollectionUtils.mkStringWithLimit(values, 10, " NOT IN (", ", ", ")")
}

case class DimIdInExpr(expr: DimensionExpr, values: SortedSetIterator[Long]) extends Expression {
  override type Out = Boolean
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override def kind: ExprKind = expr.kind

  override def fold[O](z: O)(f: (O, Expression) => O): O = expr.fold(f(z, this))(f)

  override def encode: String = s"idIn(${expr.encode}, (Iterator))"
  override def toString: String = expr.toString + " ID IN (Iterator)"
}

case class DimIdNotInExpr(expr: DimensionExpr, values: SortedSetIterator[Long]) extends Expression {
  override type Out = Boolean
  override def dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override def kind: ExprKind = expr.kind

  override def fold[O](z: O)(f: (O, Expression) => O): O = expr.fold(f(z, this))(f)

  override def encode: String = s"idNotIn(${expr.encode}, (Iterator))"
  override def toString: String = expr.toString + " ID NOT IN (Iterator)"
}

case class AndExpr(conditions: Seq[Condition]) extends Expression {
  override type Out = Boolean
  override val dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override def kind: ExprKind = conditions.foldLeft(Const: ExprKind)((k, c) => ExprKind.combine(k, c.kind))

  override def fold[O](z: O)(f: (O, Expression) => O): O = conditions.foldLeft(f(z, this))((a, e) => e.fold(a)(f))

  override def toString: String = conditions.mkString("(", " AND ", ")")
  override def encode: String = conditions.map(_.encode).sorted.mkString("and(", ",", ")")
}

case class OrExpr(conditions: Seq[Condition]) extends Expression {
  override type Out = Boolean
  override val dataType: DataType.Aux[Boolean] = DataType[Boolean]
  override def kind: ExprKind = conditions.foldLeft(Const: ExprKind)((k, c) => ExprKind.combine(k, c.kind))

  override def fold[O](z: O)(f: (O, Expression) => O): O = conditions.foldLeft(f(z, this))((a, e) => e.fold(a)(f))

  override def toString: String = conditions.mkString("(", " OR ", ")")
  override def encode: String = conditions.map(_.encode).sorted.mkString("or(", ",", ")")
}
