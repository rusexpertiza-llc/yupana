package org.yupana.api.query

import org.yupana.api.Time
import org.yupana.api.schema.{ExternalLink, Measure}
import org.yupana.api.types._

sealed trait ExprKind
case object Const extends ExprKind
case object Simple extends ExprKind
case object Aggregate extends ExprKind
case object Window extends ExprKind

sealed trait Expression extends Serializable {
  type Out

  def dataType: DataType.Aux[Out]

  def requiredDimensions: Set[String]
  def requiredLinks: Set[LinkExpr]
  def requiredMeasures: Set[Measure]

  def kind: ExprKind = Simple

  def as(name: String) = QueryField(name, this)

  def encode: String

  lazy val flatten: Set[Expression] = Set(this)

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

trait WindowFunctionExpr extends Expression {
  type In
  val operation: WindowOperation[In]
  val expr: Expression.Aux[In]

  override def requiredDimensions: Set[String] = expr.requiredDimensions
  override def requiredLinks: Set[LinkExpr] = expr.requiredLinks
  override def requiredMeasures: Set[Measure] = expr.requiredMeasures

  override def kind: ExprKind = Window

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

trait AggregateExpr extends Expression {
  type In
  val aggregation: Aggregation[In]
  val expr: Expression.Aux[In]

  override def requiredDimensions: Set[String] = expr.requiredDimensions
  override def requiredLinks: Set[LinkExpr] = expr.requiredLinks
  override def requiredMeasures: Set[Measure] = expr.requiredMeasures

  override def kind: ExprKind = Aggregate

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

trait ConstantExpr extends Expression {
  def v: Out
  override def encode: String = s"const($v)"
  override def kind: ExprKind = Const
  override def requiredDimensions: Set[String] = Set.empty
  override def requiredLinks: Set[LinkExpr] = Set.empty
  override def requiredMeasures: Set[Measure] = Set.empty
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
  override def requiredDimensions: Set[String] = Set.empty
  override def requiredLinks: Set[LinkExpr] = Set.empty
  override def requiredMeasures: Set[Measure] = Set.empty
  override def encode: String = s"time()"
  def toField = QueryField("time", this)
}

class DimensionExpr(val dimName: String) extends Expression {
  override type Out = String
  override val dataType: DataType.Aux[String] = DataType[String]
  override def requiredDimensions: Set[String] = Set(dimName)
  override def requiredLinks: Set[LinkExpr] = Set.empty
  override def requiredMeasures: Set[Measure] = Set.empty
  override def encode: String = s"dim($dimName)"
  def toField = QueryField(dimName, this)
}

object DimensionExpr {
  def apply(tagName: String): DimensionExpr = new DimensionExpr(tagName)
  def unapply(expr: DimensionExpr): Option[String] = Some(expr.dimName)
}

case class MeasureExpr[T](measure: Measure.Aux[T]) extends Expression {
  override type Out = T
  override def dataType: DataType.Aux[measure.T] = measure.dataType
  override def requiredMeasures: Set[Measure] = Set(measure)
  override def requiredDimensions: Set[String] = Set.empty
  override def requiredLinks: Set[LinkExpr] = Set.empty
  override def encode: String = s"measure(${measure.name})"
  def toField = QueryField(measure.name, this)
}

class LinkExpr(val link: ExternalLink, val linkField: String) extends Expression {
  override type Out = String
  override val dataType: DataType.Aux[String] = DataType[String]
  override def requiredDimensions: Set[String] = Set(link.dimensionName)
  override def requiredLinks: Set[LinkExpr] = Set(this)
  override def requiredMeasures: Set[Measure] = Set.empty
  override def encode: String = s"link(${link.linkName}, $linkField)"
  def queryFieldName: String = link.linkName + "_" + linkField
  def toField = QueryField(queryFieldName, this)
}

object LinkExpr {
  def apply(catalog: ExternalLink, field: String): Expression.Aux[String] = new LinkExpr(catalog, field)
  def unapply(expr: LinkExpr): Option[(ExternalLink, String)] = Some((expr.link, expr.linkField))
}

case class FunctionExpr[T, U](f: T => U,
                              override val dataType: DataType.Aux[U],
                              fun: String,
                              expr: Expression.Aux[T]) extends Expression {
  override type Out = U
  override def requiredMeasures: Set[Measure] = expr.requiredMeasures
  override def requiredDimensions: Set[String] = expr.requiredDimensions
  override def requiredLinks: Set[LinkExpr] = expr.requiredLinks
  override def encode: String =  s"$fun($expr)"

  override def kind: ExprKind = expr.kind

  override lazy val flatten: Set[Expression] = Set(this) ++ expr.flatten
}

object FunctionExpr {
  def apply[T, U](tc: TypeConverter[T, U], expr: Expression.Aux[T]): FunctionExpr[T, U] =
    FunctionExpr(tc.direct, tc.dataType, tc.functionName, expr)

  def apply[T, U](unary: UnaryOperation.Aux[T, U], expr: Expression.Aux[T]): FunctionExpr[T, U] =
    FunctionExpr(unary.apply, unary.dataType, unary.name, expr)
}

case class BinaryOperationExpr[T, U, O](function: BinaryOperation.Aux[T, U, O],
                                        a: Expression.Aux[T],
                                        b: Expression.Aux[U]) extends Expression {
  override type Out = O
  override def dataType: DataType.Aux[Out] = function.dataType
  override def requiredMeasures: Set[Measure] = a.requiredMeasures union b.requiredMeasures
  override def requiredDimensions: Set[String] = a.requiredDimensions union b.requiredDimensions
  override def requiredLinks: Set[LinkExpr] = a.requiredLinks union b.requiredLinks

  override def toString: String = s"$a $function $b"
  override def encode: String = s"$function(${a.encode}, ${b.encode})"

  override def kind: ExprKind = (a.kind, b.kind) match {
    case (Const, Const) => Const
    case (Simple, _) => Simple
    case (_, Simple) => Simple
    case (Aggregate, _) => Aggregate
    case (_, Aggregate) => Aggregate
    case (Window, _) => Window
    case (_, Window) => Window
  }

  override lazy val flatten: Set[Expression] = Set(this) ++ a.flatten ++ b.flatten
}

case class TupleExpr[T, U](e1: Expression.Aux[T], e2: Expression.Aux[U])(implicit rtt: DataType.Aux[T], rtu: DataType.Aux[U]) extends Expression {
  override type Out = (T, U)

  override def dataType: DataType.Aux[(T, U)] = DataType[(T, U)]

  override def encode: String = s"($e1, $e2)"

  override lazy val flatten: Set[Expression] = e1.flatten ++ e2.flatten + this

  override def requiredDimensions: Set[String] = e1.requiredDimensions ++ e2.requiredDimensions
  override def requiredLinks: Set[LinkExpr] = e1.requiredLinks ++ e2.requiredLinks
  override def requiredMeasures: Set[Measure] = Set.empty
}

case class ConditionExpr[T](condition: Condition, positive: Expression.Aux[T], negative: Expression.Aux[T]) extends Expression {
  override type Out = T
  override def dataType: DataType.Aux[T] = positive.dataType

  override def requiredDimensions: Set[String] =
    positive.requiredDimensions ++ negative.requiredDimensions ++ condition.exprs.flatMap(_.requiredDimensions)
  override def requiredLinks: Set[LinkExpr] =
    positive.requiredLinks ++ negative.requiredLinks ++ condition.exprs.flatMap(_.requiredLinks)
  override def requiredMeasures: Set[Measure] =
    positive.requiredMeasures ++ negative.requiredMeasures ++ condition.exprs.flatMap(_.requiredMeasures)

  override def toString: String = s"IF ($condition) THEN $positive ELSE $negative"

  override def encode: String = s"if(${condition.encoded},${positive.encode},${negative.encode}"

  override lazy val flatten: Set[Expression] = Set(this) ++
    condition.exprs.flatMap(_.flatten) ++
    positive.flatten ++
    negative.flatten
}
