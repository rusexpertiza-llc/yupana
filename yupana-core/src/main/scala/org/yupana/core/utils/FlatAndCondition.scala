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

/**
  * AND condition which contains only simple conditions and time bounds.
  * @param from start time of the condition
  * @param to end time of the condition
  * @param conditions sequence of conditions
  */
case class FlatAndCondition(from: Long, to: Long, conditions: Seq[SimpleCondition]) {
  def toCondition: Condition = {
    import org.yupana.api.query.syntax.All._

    QueryOptimizer.simplifyCondition(
      AndExpr(
        Seq(ge(time, const(Time(from))), lt(time, const(Time(to)))) ++ conditions
      )
    )
  }

  override def hashCode(): Int = encoded.hashCode

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: FlatAndCondition => this.encoded == that.encoded
      case _                      => false
    }
  }

  private lazy val encoded: String = {
    val cEnc = conditions.map(_.encode).sorted.mkString(",")
    s"$from,$to,$cEnc"
  }
}

object FlatAndCondition {

  private case class FlatAndConditionParts(from: Option[Long], to: Option[Long], conditions: Seq[SimpleCondition])

  def apply(constantCalculator: ConstantCalculator, condition: Condition): Seq[FlatAndCondition] = {
    val parts = fromCondition(constantCalculator, Seq.empty, condition)
    val (errs, result) = parts.partitionMap { fac =>
      for {
        from <- fac.from.toRight(s"FROM time is not defined for ${AndExpr(fac.conditions)}")
        to <- fac.to.toRight(s"TO time is not defined for ${AndExpr(fac.conditions)}")
      } yield FlatAndCondition(from, to, fac.conditions)
    }

    if (errs.isEmpty) result else throw new IllegalArgumentException(errs.mkString(", "))
  }

  def single(constantCalculator: ConstantCalculator, condition: Condition): FlatAndCondition = {
    FlatAndCondition(constantCalculator, condition) match {
      case Seq(tbc) =>
        tbc
      case _ =>
        throw new IllegalArgumentException("Using of several TimeBoundedCondition are unsupported!")
    }
  }

  def mergeByTime(tbcs: Seq[FlatAndCondition]): Seq[(Long, Long, Option[Condition])] = {
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

  private def fromCondition(
      expressionCalculator: ConstantCalculator,
      tbcs: Seq[FlatAndConditionParts],
      condition: Condition
  ): Seq[FlatAndConditionParts] = {

    def update(f: FlatAndConditionParts => FlatAndConditionParts): Seq[FlatAndConditionParts] = {
      if (tbcs.isEmpty) Seq(f(FlatAndConditionParts(None, None, Seq.empty))) else tbcs.map(f)
    }

    def updateFrom(c: SimpleCondition, e: Expression[Time], offset: Long): Seq[FlatAndConditionParts] = {
      if (e.kind == Const) {
        val const = expressionCalculator.evaluateConstant(e)
        update(t =>
          t.copy(from = t.from.map(o => math.max(const.millis + offset, o)) orElse Some(const.millis + offset))
        )
      } else {
        update(t => t.copy(conditions = t.conditions :+ c))
      }
    }

    def updateTo(c: SimpleCondition, e: Expression[Time], offset: Long): Seq[FlatAndConditionParts] = {
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

      case AndExpr(cs) => cs.foldLeft(tbcs)((t, c) => fromCondition(expressionCalculator, t, c))

      case OrExpr(cs) => cs.flatMap(c => fromCondition(expressionCalculator, tbcs, c))

      case x: SimpleCondition => update(t => t.copy(conditions = t.conditions :+ x))

      case ConstantExpr(true, _)  => tbcs
      case ConstantExpr(false, _) => update(t => t.copy(conditions = t.conditions :+ FalseExpr))

      case x => throw new IllegalArgumentException(s"Unexpected condition $x")
    }
  }
}
