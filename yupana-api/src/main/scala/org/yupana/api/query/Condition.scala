package org.yupana.api.query

import org.yupana.api.Time
import org.yupana.api.utils.CollectionUtils
import org.yupana.api.types.Comparison

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
      Compare(Comparison.ge[Time], TimeExpr, from),
      Compare(Comparison.lt[Time], TimeExpr, to)
    ) ++ condition)
  }
}

case object EmptyCondition extends Condition {
  override def exprs: Set[Expression] = Set.empty
  override def encode: String = "empty"
}

trait Compare extends Condition {
  type T
  def c: Comparison[T]
  def a: Expression.Aux[T]
  def b: Expression.Aux[T]
}

object Compare {
  def apply[A](comparison: Comparison[A], left: Expression.Aux[A], right: Expression.Aux[A]): Compare  = new Compare() {
    override type T = A
    override lazy val c: Comparison[T] = comparison
    override lazy val a: Expression.Aux[T] = left
    override lazy val b: Expression.Aux[T] = right

    override def exprs: Set[Expression] = Set(left, right)

    override def toString: String = s"$a $c $b"
    override def encode: String = s"$c(${a.encode}, ${b.encode})"
  }

  def unapply(c: Compare): Option[(Comparison[c.T], Expression.Aux[c.T], Expression.Aux[c.T])] = Some((c.c, c.a, c.b))
}

case class IsNull(e: Expression) extends Condition {
    override def exprs: Set[Expression] = Set(e) // todo add Expr::exprs and use e.exprs here
    override def encode: String = s"isNull(${e.encode})"
    override def toString: String = s"$e IS NULL"
}

case class IsNotNull(e: Expression) extends Condition {
  override def exprs: Set[Expression] = Set(e) // todo add Expr::exprs and use e.exprs here
  override def encode: String = s"isNotNull(${e.encode})"
  override def toString: String = s"$e  IS NOT NULL"
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

case class TagIdIn(expr: DimensionExpr, tagIds: Set[Int]) extends Condition {
  override def exprs: Set[Expression] = Set(expr)
  override def encode: String = tagIds.mkString(s"idIn(${expr.encode}, (", ",","))")
  override def toString: String = {
    expr.toString + CollectionUtils.mkStringWithLimit(tagIds, 10, " ID IN (", ", ", ")")
  }
}

case class TagIdNotIn(expr: DimensionExpr, tagIds: Set[Int]) extends Condition {
  override def exprs: Set[Expression] = Set(expr)
  override def encode: String = tagIds.mkString(s"idNotIn(${expr.encode}, (", ",","))")
  override def toString: String = {
    expr.toString + CollectionUtils.mkStringWithLimit(tagIds, 10, " ID NOT IN (", ", ", ")")
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
