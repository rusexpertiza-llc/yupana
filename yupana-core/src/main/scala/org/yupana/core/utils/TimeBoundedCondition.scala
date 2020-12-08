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

package org.yupana.core.utils

import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.utils.ConditionMatchers.{ GeMatcher, GtMatcher, LeMatcher, LtMatcher }
import org.yupana.core.{ ExpressionCalculator, QueryOptimizer }

import scala.collection.mutable.ListBuffer

case class TimeBoundedCondition(from: Option[Long], to: Option[Long], conditions: Seq[Condition]) {
  def toCondition: Condition = {
    import org.yupana.api.query.syntax.All._

    QueryOptimizer.simplifyCondition(
      AndExpr(
        Seq(
          from.map(f => ge(time, const(Time(f)))).getOrElse(ConstantExpr(true)),
          to.map(t => lt(time, const(Time(t)))).getOrElse(ConstantExpr(true))
        ) ++ conditions
      )
    )
  }
}

object TimeBoundedCondition {

  def apply(expressionCalculator: ExpressionCalculator, condition: Condition): Seq[TimeBoundedCondition] = {
    condition match {
      case a: AndExpr => andToTimeBounded(expressionCalculator)(a)
      case x          => Seq(TimeBoundedCondition(None, None, Seq(x)))
    }
  }

  def apply(from: Long, to: Long, condition: Condition): TimeBoundedCondition = {
    QueryOptimizer.simplifyCondition(condition) match {
      case AndExpr(cs) => TimeBoundedCondition(Some(from), Some(to), cs)
      case o: OrExpr   => throw new IllegalArgumentException(s"Or not supported yet $o")
      case c           => TimeBoundedCondition(Some(from), Some(to), Seq(c))
    }
  }

  def toCondition(conditions: Seq[TimeBoundedCondition]): Condition = {
    conditions.map(_.toCondition) match {
      case Nil    => ConstantExpr(true)
      case Seq(x) => x
      case xs     => OrExpr(xs)
    }
  }

  def merge(conditions: Seq[TimeBoundedCondition]): TimeBoundedCondition = {
    if (conditions.isEmpty) throw new IllegalArgumentException("Conditions must not be empty")

    val from = conditions.head.from
    val to = conditions.head.to
    val cs = conditions.foldLeft(Seq.empty[Condition])((a, c) =>
      if (c.from == from && c.to == to) {
        a ++ c.conditions
      } else {
        throw new IllegalArgumentException("Conditions must have same time limits.")
      }
    )
    TimeBoundedCondition(from, to, cs)
  }

  private object GtTime extends GtMatcher[Time]
  private object LtTime extends LtMatcher[Time]
  private object GeTime extends GeMatcher[Time]
  private object LeTime extends LeMatcher[Time]

  private def andToTimeBounded(expressionCalculator: ExpressionCalculator)(and: AndExpr): Seq[TimeBoundedCondition] = {
    var from = Option.empty[Long]
    var to = Option.empty[Long]
    val other = ListBuffer.empty[Condition]

    def updateFrom(c: Condition, e: Expression[_], offset: Long): Unit = {
      val const = expressionCalculator.evaluateConstant(e.asInstanceOf[Expression[Time]])
      if (const != null) {
        from = from.map(o => math.max(const.millis + offset, o)) orElse Some(const.millis + offset)
      } else {
        other += c
      }
    }

    def updateTo(c: Condition, e: Expression[_], offset: Long): Unit = {
      val const = expressionCalculator.evaluateConstant(e.asInstanceOf[Expression[Time]])
      if (const != null) {
        to = to.map(o => math.min(const.millis + offset, o)) orElse Some(const.millis)
      } else {
        other += c
      }
    }

    and.conditions.foreach {
      case c @ GtTime(TimeExpr, e) => updateFrom(c, e, 1L)
      case c @ LtTime(e, TimeExpr) => updateFrom(c, e, 1L)

      case c @ GeTime(TimeExpr, e) => updateFrom(c, e, 0L)
      case c @ LeTime(e, TimeExpr) => updateFrom(c, e, 0L)

      case c @ LtTime(TimeExpr, e) => updateTo(c, e, 0L)
      case c @ GtTime(e, TimeExpr) => updateTo(c, e, 0L)

      case c @ LeTime(TimeExpr, e) => updateTo(c, e, 1L)
      case c @ GeTime(e, TimeExpr) => updateTo(c, e, 1L)

      case c @ (AndExpr(_) | OrExpr(_)) => throw new IllegalArgumentException(s"Unexpected condition $c")

      case x => other += x
    }

    Seq(TimeBoundedCondition(from, to, other.toList))
  }
}
