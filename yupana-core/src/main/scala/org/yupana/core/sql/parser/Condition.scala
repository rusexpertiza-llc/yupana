package org.yupana.core.sql.parser

sealed trait Condition {
  def simplify: Condition = this
}

case class IsNull(e: SqlExpr) extends Condition
case class IsNotNull(e: SqlExpr) extends Condition

sealed trait Comparison extends Condition {
  val a: SqlExpr
  val b: SqlExpr
}

case class Eq(a: SqlExpr, b: SqlExpr) extends Comparison
case class Ne(a: SqlExpr, b: SqlExpr) extends Comparison
case class Gt(a: SqlExpr, b: SqlExpr) extends Comparison
case class Ge(a: SqlExpr, b: SqlExpr) extends Comparison
case class Lt(a: SqlExpr, b: SqlExpr) extends Comparison
case class Le(a: SqlExpr, b: SqlExpr) extends Comparison

case class And(cs: Seq[Condition]) extends Condition {
  override def simplify: Condition = {
    And(cs.flatMap(c => c match {
      case And(ccs) => ccs map (_.simplify)
      case x => Seq(x)
    }))
  }
}

case class Or(cs: Seq[Condition]) extends Condition {
  override def simplify: Condition = {
    Or(cs.flatMap(c => c match {
      case Or(ccs) => ccs map (_.simplify)
      case x => Seq(x)
    }))
  }
}

case class In(expr: SqlExpr, values: Seq[Value]) extends Condition
case class NotIn(expr: SqlExpr, values: Seq[Value]) extends Condition

case class ExprCondition(function: SqlExpr) extends Condition
