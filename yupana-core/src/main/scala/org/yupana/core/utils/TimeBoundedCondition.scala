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

  def optimize: TimeBoundedCondition = {
    def flat(cs: Seq[Condition]): Seq[Condition] = {
      cs.flatMap {
        case AndExpr(xs) => flat(xs)
        case x           => Seq(x)
      }
    }

    this.copy(from = this.from, to = this.to, conditions = flat(this.conditions))
  }
}

object TimeBoundedCondition {

  def apply(constantCalculator: ConstantCalculator, condition: Condition): Seq[TimeBoundedCondition] = {
    toTimeBounded(constantCalculator, Seq.empty, condition)
  }

  def single(constantCalculator: ConstantCalculator, condition: Condition): TimeBoundedCondition = {
    TimeBoundedCondition(constantCalculator, condition) match {
      case Seq(tbc) =>
        tbc
      case _ =>
        throw new IllegalArgumentException("Using of several TimeBoundedCondition are unsupported!")
    }
  }

  def mergeByTime(tbcs: Seq[TimeBoundedCondition]): Seq[(Option[Long], Option[Long], Option[Condition])] = {
    tbcs
      .groupBy(tbc => (tbc.from, tbc.to))
      .map {
        case ((f, t), cs) =>
          val c = OrExpr(cs.map(x => AndExpr(x.conditions)))
          val simplified = QueryOptimizer.simplifyCondition(c)

          (f, t, if (simplified == ConstantExpr(true)) None else Some(simplified))
      }
      .toSeq
  }

  private object GtTime extends GtMatcher[Time]
  private object LtTime extends LtMatcher[Time]
  private object GeTime extends GeMatcher[Time]
  private object LeTime extends LeMatcher[Time]

  private def toTimeBounded(
      expressionCalculator: ConstantCalculator,
      tbcs: Seq[TimeBoundedCondition],
      condition: Condition
  ): Seq[TimeBoundedCondition] = {

    def update(f: TimeBoundedCondition => TimeBoundedCondition): Seq[TimeBoundedCondition] = {
      if (tbcs.isEmpty) Seq(f(TimeBoundedCondition(None, None, Seq.empty))) else tbcs.map(f)
    }

    def updateFrom(c: Condition, e: Expression[Time], offset: Long): Seq[TimeBoundedCondition] = {
      if (e.kind == Const) {
        val const = expressionCalculator.evaluateConstant(e)
        update(t =>
          t.copy(from = t.from.map(o => math.max(const.millis + offset, o)) orElse Some(const.millis + offset))
        )
      } else {
        update(t => t.copy(conditions = t.conditions :+ c))
      }
    }

    def updateTo(c: Condition, e: Expression[Time], offset: Long): Seq[TimeBoundedCondition] = {
      if (e.kind == Const) {
        val const = expressionCalculator.evaluateConstant(e)
        update(t => t.copy(to = t.to.map(o => math.min(const.millis + offset, o)) orElse Some(const.millis + offset)))
      } else {
        update(t => t.copy(conditions = t.conditions :+ c))
      }
    }

    condition match {
      case c @ GtTime(TimeExpr, e) => updateFrom(c, e, 1L)
      case c @ LtTime(e, TimeExpr) => updateFrom(c, e, 1L)

      case c @ GeTime(TimeExpr, e) => updateFrom(c, e, 0L)
      case c @ LeTime(e, TimeExpr) => updateFrom(c, e, 0L)

      case c @ LtTime(TimeExpr, e) => updateTo(c, e, 0L)
      case c @ GtTime(e, TimeExpr) => updateTo(c, e, 0L)

      case c @ LeTime(TimeExpr, e) => updateTo(c, e, 1L)
      case c @ GeTime(e, TimeExpr) => updateTo(c, e, 1L)

      case AndExpr(cs) => cs.foldLeft(tbcs)((t, c) => toTimeBounded(expressionCalculator, t, c))

      case OrExpr(cs) => cs.flatMap(c => toTimeBounded(expressionCalculator, tbcs, c))

      case x => update(t => t.copy(conditions = t.conditions :+ x))
    }
  }
}
