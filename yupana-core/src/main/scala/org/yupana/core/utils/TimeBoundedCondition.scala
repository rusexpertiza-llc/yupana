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
import org.yupana.core.{ ConstantCalculator, QueryOptimizer }

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

  def apply(constantCalculator: ConstantCalculator, condition: Condition): Seq[TimeBoundedCondition] = {
    condition match {
      case a: AndExpr => andToTimeBounded(constantCalculator)(a)
      case x          => Seq(TimeBoundedCondition(None, None, Seq(x)))
    }
  }

  def single(constantCalculator: ConstantCalculator, condition: Condition): TimeBoundedCondition = {
    TimeBoundedCondition(constantCalculator, condition) match {
      case Seq(tbc) =>
        tbc
      case _ =>
        throw new IllegalArgumentException("Using of several TimeBoundedCondition are unsupported!")
    }
  }

  private object GtTime extends GtMatcher[Time]
  private object LtTime extends LtMatcher[Time]
  private object GeTime extends GeMatcher[Time]
  private object LeTime extends LeMatcher[Time]

  private def andToTimeBounded(expressionCalculator: ConstantCalculator)(and: AndExpr): Seq[TimeBoundedCondition] = {
    var from = Option.empty[Long]
    var to = Option.empty[Long]
    val other = ListBuffer.empty[Condition]

    def updateFrom(c: Condition, e: Expression[Time], offset: Long): Unit = {
      if (e.kind == Const) {
        val const = expressionCalculator.evaluateConstant(e)
        from = from.map(o => math.max(const.millis + offset, o)) orElse Some(const.millis + offset)
      } else {
        other += c
      }
    }

    def updateTo(c: Condition, e: Expression[Time], offset: Long): Unit = {
      if (e.kind == Const) {
        val const = expressionCalculator.evaluateConstant(e)
        to = to.map(o => math.min(const.millis + offset, o)) orElse Some(const.millis + offset)
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
