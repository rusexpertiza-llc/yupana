package org.yupana.core.utils

import org.yupana.api.Time
import org.yupana.api.query._
import org.yupana.core.ExpressionCalculator
import org.yupana.core.utils.ConditionMatchers._

import scala.collection.mutable.ListBuffer

case class TimeBoundedCondition(from: Option[Long], to: Option[Long], conditions: Seq[Condition]) {
  def toCondition: Condition = {
    import org.yupana.api.query.syntax.All._

    ConditionUtils.simplify(And(Seq(
      from.map(f => ge(time, const(Time(f)))).getOrElse(EmptyCondition),
      to.map(t => lt(time, const(Time(t)))).getOrElse(EmptyCondition)
    ) ++ conditions))
  }
}

object TimeBoundedCondition {

  def apply(condition: Condition): Seq[TimeBoundedCondition] = {
    condition match {
      case a: And => andToTimeBounded(a)
      case x => Seq(TimeBoundedCondition(None, None, Seq(x)))
    }
  }

  def apply(from: Long, to: Long, condition: Condition): TimeBoundedCondition = {
    ConditionUtils.simplify(condition) match {
      case And(cs) => TimeBoundedCondition(Some(from), Some(to), cs)
      case o: Or => throw new IllegalArgumentException(s"Or not supported yet $o")
      case c => TimeBoundedCondition(Some(from), Some(to), Seq(c))
    }
  }

  def toCondition(conditions: Seq[TimeBoundedCondition]): Condition = {
    conditions.map(_.toCondition) match {
      case Nil => EmptyCondition
      case Seq(x) => x
      case xs => Or(xs)
    }
  }

  def merge(conditions: Seq[TimeBoundedCondition]): TimeBoundedCondition = {
    if (conditions.isEmpty) throw new IllegalArgumentException("Conditions must not be empty")

    val from = conditions.head.from
    val to = conditions.head.to
    val cs = conditions.foldLeft(Seq.empty[Condition])((a, c) => if (c.from == from && c.to == to) {
      a ++ c.conditions
    } else {
      throw new IllegalArgumentException("Conditions must have same time limits.")
    })
    TimeBoundedCondition(from, to, cs)
  }

  private def andToTimeBounded(and: And): Seq[TimeBoundedCondition] = {
    var from = Option.empty[Long]
    var to = Option.empty[Long]
    var other = ListBuffer.empty[Condition]

    def updateFrom(c: Condition, e: Expression, offset: Long): Unit = {
      val const = ExpressionCalculator.evaluateExpression(e.asInstanceOf[Expression.Aux[Time]], null, null)
      const match {
        case Some(t) => from = from.map(o => math.max(t.millis + offset, o)) orElse Some(t.millis + offset)
        case _ => other += c
      }
    }

    def updateTo(c: Condition, e: Expression, offset: Long): Unit = {
      val const = ExpressionCalculator.evaluateExpression(e.asInstanceOf[Expression.Aux[Time]], null, null)
      const match {
        case Some(t) => to = to.map(o => math.max(t.millis + offset, o)) orElse Some(t.millis)
        case _ => other += c
      }
    }

    and.conditions.foreach {
      case c@Gt(TimeExpr, e) => updateFrom(c, e, 1l)
      case c@Lt(e, TimeExpr) => updateFrom(c, e, 1l)

      case c@Ge(TimeExpr, e) => updateFrom(c, e, 0l)
      case c@Le(e, TimeExpr) => updateFrom(c, e, 0l)

      case c@Lt(TimeExpr, e) => updateTo(c, e, 0l)
      case c@Gt(e, TimeExpr) => updateTo(c, e, 0l)

      case c@Le(TimeExpr, e) => updateTo(c, e, 1l)
      case c@Ge(e, TimeExpr) => updateTo(c, e, 1l)

      case c@(And(_) | Or(_)) => throw new IllegalArgumentException(s"Unexpected condition $c")

      case x => other += x
    }

    Seq(TimeBoundedCondition(from, to, other.toList))
  }
}
