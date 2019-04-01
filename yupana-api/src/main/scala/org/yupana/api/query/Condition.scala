package org.yupana.api.query

import org.yupana.api.Time
import org.yupana.api.types.BinaryOperation
import org.yupana.api.utils.CollectionUtils

sealed trait Condition extends Serializable {
  def exprs: Set[Expression]
  val encoded: String = encode
  val encodedHashCode: Int = encoded.hashCode()
  def encode: String

  override def hashCode(): Int = encodedHashCode

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case that: Condition => this.encoded == that.encoded
      case _ => false
    }
  }
}

object Condition {
  def and(conditions: Seq[Condition]): Condition = {
    val nonEmpty = conditions.filterNot(_ == EmptyCondition)
    if (nonEmpty.size == 1) {
      nonEmpty.head
    } else if (nonEmpty.nonEmpty) {
      And(nonEmpty)
    } else {
      EmptyCondition
    }
  }

  def or(conditions: Seq[Condition]): Condition = {
    val nonEmpty = conditions.filterNot(_ == EmptyCondition)
    if (nonEmpty.size == 1) {
      nonEmpty.head
    } else if (nonEmpty.nonEmpty) {
      Or(nonEmpty)
    } else {
      EmptyCondition
    }
  }

  def timeAndCondition(from: Expression.Aux[Time], to: Expression.Aux[Time], condition: Option[Condition]): Condition = {
    And(Seq(
      SimpleCondition(BinaryOperationExpr(BinaryOperation.ge[Time], TimeExpr, from)),
      SimpleCondition(BinaryOperationExpr(BinaryOperation.lt[Time], TimeExpr, to))
    ) ++ condition)
  }
}

case object EmptyCondition extends Condition {
  override def exprs: Set[Expression] = Set.empty
  override def encode: String = "empty"
}

case class SimpleCondition(e: Expression.Aux[Boolean]) extends Condition {
  override def exprs: Set[Expression] = Set(e)
  override def encode: String = e.encode
  override def toString: String = e.toString
}

trait In extends Condition {
  type T
  def e: Expression.Aux[T]
  def vs: Set[T]
}

object In {
  def apply[A](expr: Expression.Aux[A], values: Set[A]): In = new In() {
    override type T = A

    override lazy val e: Expression.Aux[T] = expr

    override lazy val vs: Set[T] = values

    override def exprs: Set[Expression] = Set(expr)

    override def encode: String = values.toSeq.map(_.toString).sorted.mkString(s"in(${expr.encode}, (", ",","))")
    override def toString: String = {
      expr.toString + CollectionUtils.mkStringWithLimit(values, 10, " IN (", ", ", ")")
    }
  }

  def unapply(in: In): Option[(Expression.Aux[in.T], Set[in.T])] = Some((in.e, in.vs))
}

trait NotIn extends Condition {
  type T
  def e: Expression.Aux[T]
  def vs: Set[T]
}

object NotIn {
  def apply[A](expr: Expression.Aux[A], values: Set[A]): NotIn = new NotIn() {
    override type T = A

    override lazy val e: Expression.Aux[T] = expr

    override lazy val vs: Set[T] = values

    override def exprs: Set[Expression] = Set(expr)

    override def encode: String = values.toSeq.map(_.toString).sorted.mkString(s"nin(${expr.encode}, (", ",","))")
    override def toString: String = {
      expr.toString + CollectionUtils.mkStringWithLimit(values, 10, " NOT IN (", ", ", ")")
    }
  }

  def unapply(nin: NotIn): Option[(Expression.Aux[nin.T], Set[nin.T])] = Some((nin.e, nin.vs))
}

case class DimIdIn(expr: DimensionExpr, dimIds: Set[Int]) extends Condition {
  override def exprs: Set[Expression] = Set(expr)
  override def encode: String = dimIds.mkString(s"idIn(${expr.encode}, (", ",","))")
  override def toString: String = {
    expr.toString + CollectionUtils.mkStringWithLimit(dimIds, 10, " ID IN (", ", ", ")")
  }
}

case class DimIdNotIn(expr: DimensionExpr, dimIds: Set[Int]) extends Condition {
  override def exprs: Set[Expression] = Set(expr)
  override def encode: String = dimIds.mkString(s"idNotIn(${expr.encode}, (", ",","))")
  override def toString: String = {
    expr.toString + CollectionUtils.mkStringWithLimit(dimIds, 10, " ID NOT IN (", ", ", ")")
  }
}

case class And(conditions: Seq[Condition]) extends Condition {

  override def exprs: Set[Expression] = conditions.foldLeft(Set.empty[Expression])(_ union _.exprs)

  override def toString: String = conditions.mkString("(", " AND ", ")")

  override def encode: String = conditions.map(_.encoded).sorted.mkString("and(", ",", ")")
}

case class Or(conditions: Seq[Condition]) extends Condition {

  override def exprs: Set[Expression] = conditions.foldLeft(Set.empty[Expression])(_ union _.exprs)

  override def toString: String = conditions.mkString( "(", " OR ", ")")
  override def encode: String = conditions.map(_.encoded).sorted.mkString("or(", ",", ")")
}
