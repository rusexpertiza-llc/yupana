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
import org.yupana.api.schema.{Dimension, ExternalLink, Metric}
import org.yupana.api.types._
import org.yupana.api.utils.CollectionUtils

sealed trait Expression extends Serializable {
  type Out

  def dataType: DataType.Aux[Out]

  def requiredDimensions: Set[Dimension]
  def requiredLinks: Set[LinkExpr]
  def requiredMetrics: Set[Metric]

  def kind: ExprKind

  def as(name: String) = QueryField(name, this)

  def encode: String

  def aux: Expression.Aux[Out] = this.asInstanceOf[Expression.Aux[Out]]

  lazy val flatten: Set[Expression]  = Set(this)

  def containsAggregates: Boolean = flatten.exists {
    case _: AggregateExpr => true
    case _ => false
  }

  def containsWindows: Boolean = flatten.exists {
    case _: WindowFunctionExpr => true
    case _ => false
  }

  private lazy val encoded = encode
  private lazy val encodedHashCode = encoded.hashCode()

  override def toString: String = encoded

  override def hashCode(): Int = encodedHashCode

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case that: Expression => this.encoded == that.encoded
      case _ => false
    }
  }
}

object Expression {
  type Aux[T] = Expression { type Out = T }
}

sealed trait WindowFunctionExpr extends Expression {
  type In
  val operation: WindowOperation[In]
  val expr: Expression.Aux[In]

  override def requiredDimensions: Set[Dimension] = expr.requiredDimensions
  override def requiredLinks: Set[LinkExpr] = expr.requiredLinks
  override def requiredMetrics: Set[Metric] = expr.requiredMetrics

  override def kind: ExprKind = if (expr.kind == Simple || expr.kind == Const) Window else Invalid

  override def toString: String = s"${operation.name}($expr)"
}

object WindowFunctionExpr {
  def apply[T](op: WindowOperation[T], e: Expression.Aux[T]): WindowFunctionExpr = new WindowFunctionExpr {
    override type In = T
    override type Out = op.Out
    override def dataType: DataType.Aux[Out] = op.dataType
    override val operation: WindowOperation[T] = op
    override val expr: Expression.Aux[T] = e
    override def encode: String = s"winFunc(${op.name},${e.encode})"
  }
  def unapply(arg: WindowFunctionExpr): Option[(WindowOperation[arg.In], Expression.Aux[arg.In])] = {
    Some((arg.operation, arg.expr))
  }
}

sealed trait AggregateExpr extends Expression {
  type In
  val aggregation: Aggregation[In]
  val expr: Expression.Aux[In]

  override def requiredDimensions: Set[Dimension] = expr.requiredDimensions
  override def requiredLinks: Set[LinkExpr] = expr.requiredLinks
  override def requiredMetrics: Set[Metric] = expr.requiredMetrics

  override def kind: ExprKind = if (expr.kind == Simple || expr.kind == Const) Aggregate else Invalid

  override def toString: String = s"${aggregation.name}($expr)"

  override lazy val flatten: Set[Expression] = Set(this) ++ expr.flatten
}

object AggregateExpr {
  type Aux[T] = AggregateExpr { type Out = T }

  def apply[T](a: Aggregation[T], e: Expression.Aux[T]): AggregateExpr.Aux[a.Out] = new AggregateExpr {
    override type In = T
    override type Out = a.Out
    override def dataType: DataType.Aux[Out] = a.dataType
    override val aggregation: Aggregation[T] = a
    override val expr: Expression.Aux[T] = e
    override def encode: String = s"agg(${a.name},${e.encode})"
  }

  def unapply(arg: AggregateExpr): Option[(Aggregation[arg.In], Expression.Aux[arg.In])] = {
    Some((arg.aggregation, arg.expr))
  }
}

sealed trait ConstantExpr extends Expression {
  def v: Out
  override def encode: String = s"const($v)"
  override def kind: ExprKind = Const
  override def requiredDimensions: Set[Dimension] = Set.empty
  override def requiredLinks: Set[LinkExpr] = Set.empty
  override def requiredMetrics: Set[Metric] = Set.empty
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
  override def requiredDimensions: Set[Dimension] = Set.empty
  override def requiredLinks: Set[LinkExpr] = Set.empty
  override def requiredMetrics: Set[Metric] = Set.empty
  override def encode: String = s"time()"
  def toField = QueryField("time", this)
}

class DimensionExpr(val dimension: Dimension) extends Expression {
  override type Out = String
  override val dataType: DataType.Aux[String] = DataType[String]
  override def kind: ExprKind = Simple
  override def requiredDimensions: Set[Dimension] = Set(dimension)
  override def requiredLinks: Set[LinkExpr] = Set.empty
  override def requiredMetrics: Set[Metric] = Set.empty
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
  override def requiredMetrics: Set[Metric] = Set(metric)
  override def requiredDimensions: Set[Dimension] = Set.empty
  override def requiredLinks: Set[LinkExpr] = Set.empty
  override def encode: String = s"metric(${metric.name})"
  def toField = QueryField(metric.name, this)
}

class LinkExpr(val link: ExternalLink, val linkField: String) extends Expression {
  override type Out = String
  override val dataType: DataType.Aux[String] = DataType[String]
  override def kind: ExprKind = Simple
  override def requiredDimensions: Set[Dimension] = Set(link.dimension)
  override def requiredLinks: Set[LinkExpr] = Set(this)
  override def requiredMetrics: Set[Metric] = Set.empty
  override def encode: String = s"link(${link.linkName}, $linkField)"
  def queryFieldName: String = link.linkName + "_" + linkField
  def toField = QueryField(queryFieldName, this)
}

object LinkExpr {
  def apply(link: ExternalLink, field: String): Expression.Aux[String] = new LinkExpr(link, field)
  def unapply(expr: LinkExpr): Option[(ExternalLink, String)] = Some((expr.link, expr.linkField))
}

case class UnaryOperationExpr[T, U](function: UnaryOperation.Aux[T, U],
                                    expr: Expression.Aux[T]
                          ) extends Expression {
  override type Out = U
  override def dataType: DataType.Aux[U] = function.dataType
  override def requiredDimensions: Set[Dimension] = expr.requiredDimensions
  override def requiredLinks: Set[LinkExpr] = expr.requiredLinks
  override def requiredMetrics: Set[Metric] = expr.requiredMetrics
  override def encode: String = s"${function.name}($expr)"

  override def kind: ExprKind = expr.kind
  override lazy val flatten: Set[Expression] = Set(this) ++ expr.flatten
}

case class TypeConvertExpr[T, U](tc: TypeConverter[T, U],
                                 expr: Expression.Aux[T]
                                ) extends Expression {
  override type Out = U

  override def dataType: DataType.Aux[U] = tc.dataType
  override def requiredMetrics: Set[Metric] = expr.requiredMetrics
  override def requiredDimensions: Set[Dimension] = expr.requiredDimensions
  override def requiredLinks: Set[LinkExpr] = expr.requiredLinks
  override def encode: String =  s"${tc.functionName}($expr)"

  override def kind: ExprKind = expr.kind
  override lazy val flatten: Set[Expression] = Set(this) ++ expr.flatten
}

case class BinaryOperationExpr[T, U, O](function: BinaryOperation.Aux[T, U, O],
                                        a: Expression.Aux[T],
                                        b: Expression.Aux[U]) extends Expression {
  override type Out = O
  override def dataType: DataType.Aux[Out] = function.dataType
  override def requiredMetrics: Set[Metric] = a.requiredMetrics union b.requiredMetrics
  override def requiredDimensions: Set[Dimension] = a.requiredDimensions union b.requiredDimensions
  override def requiredLinks: Set[LinkExpr] = a.requiredLinks union b.requiredLinks

  override def toString: String = if (function.infix) s"$a $function $b" else s"$function($a, $b)"
  override def encode: String = s"$function(${a.encode}, ${b.encode})"

  override def kind: ExprKind = ExprKind.combine(a.kind, b.kind)

  override lazy val flatten: Set[Expression] = Set(this) ++ a.flatten ++ b.flatten
}

case class TupleExpr[T, U](e1: Expression.Aux[T], e2: Expression.Aux[U])(implicit rtt: DataType.Aux[T], rtu: DataType.Aux[U]) extends Expression {
  override type Out = (T, U)

  override def dataType: DataType.Aux[(T, U)] = DataType[(T, U)]

  override def kind: ExprKind = ExprKind.combine(e1.kind, e2.kind)

  override def encode: String = s"($e1, $e2)"

  override lazy val flatten: Set[Expression] = e1.flatten ++ e2.flatten + this

  override def requiredDimensions: Set[Dimension] = e1.requiredDimensions ++ e2.requiredDimensions
  override def requiredLinks: Set[LinkExpr] = e1.requiredLinks ++ e2.requiredLinks
  override def requiredMetrics: Set[Metric] = Set.empty
}


case class ArrayExpr[T](exprs: Array[Expression.Aux[T]])(implicit val elementDataType: DataType.Aux[T]) extends Expression {
  override type Out = Array[T]

  override val dataType: DataType.Aux[Array[T]] = DataType[Array[T]]

  override def kind: ExprKind = exprs.foldLeft(Const: ExprKind)((a, e) => ExprKind.combine(a, e.kind))

  override def requiredDimensions: Set[Dimension] = exprs.foldLeft(Set.empty[Dimension])(_ ++ _.requiredDimensions)
  override def requiredLinks: Set[LinkExpr] = exprs.foldLeft(Set.empty[LinkExpr])(_ ++ _.requiredLinks)
  override def requiredMetrics: Set[Metric] = exprs.foldLeft(Set.empty[Metric])(_ ++ _.requiredMetrics)

  override def encode: String = exprs.mkString("[", ", ", "]")
  override def toString: String = CollectionUtils.mkStringWithLimit(exprs)
}

case class ConditionExpr[T](condition: Condition, positive: Expression.Aux[T], negative: Expression.Aux[T]) extends Expression {
  override type Out = T
  override def dataType: DataType.Aux[T] = positive.dataType

  override def kind: ExprKind = ExprKind.combine(positive.kind, negative.kind)

  override def requiredDimensions: Set[Dimension] =
    positive.requiredDimensions ++ negative.requiredDimensions ++ condition.exprs.flatMap(_.requiredDimensions)
  override def requiredLinks: Set[LinkExpr] =
    positive.requiredLinks ++ negative.requiredLinks ++ condition.exprs.flatMap(_.requiredLinks)
  override def requiredMetrics: Set[Metric] =
    positive.requiredMetrics ++ negative.requiredMetrics ++ condition.exprs.flatMap(_.requiredMetrics)

  override def toString: String = s"IF ($condition) THEN $positive ELSE $negative"

  override def encode: String = s"if(${condition.encoded},${positive.encode},${negative.encode}"

  override lazy val flatten: Set[Expression] = Set(this) ++
    condition.exprs.flatMap(_.flatten) ++
    positive.flatten ++
    negative.flatten
}
