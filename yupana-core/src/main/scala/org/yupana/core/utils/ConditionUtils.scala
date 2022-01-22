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

import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.core.QueryOptimizer

object ConditionUtils {
  def flatMap(c: Condition)(f: Condition => Condition): Condition = {
    def doFlat(xs: Seq[Condition]): Seq[Condition] = {
      xs.flatMap(x =>
        flatMap(x)(f) match {
          case ConstantExpr(true) => None
          case nonEmpty           => Some(nonEmpty)
        }
      )
    }

    val mapped = c match {
      case AndExpr(cs) => AndExpr(doFlat(cs))
      case OrExpr(cs)  => OrExpr(doFlat(cs))
      case x           => f(x)
    }

    QueryOptimizer.simplifyCondition(mapped)
  }

  def split(c: Condition)(p: Condition => Boolean): (Condition, Condition) = {
    def doSplit(c: Condition): (Condition, Condition) = {
      c match {
        case AndExpr(cs) =>
          val (a, b) = cs.map(doSplit).unzip
          (AndExpr(a), AndExpr(b))

        case OrExpr(cs) =>
          val (a, b) = cs.map(doSplit).unzip
          (OrExpr(a), OrExpr(b))

        case x => if (p(x)) (x, ConstantExpr(true)) else (ConstantExpr(true), x)
      }
    }

    val (a, b) = doSplit(c)

    (QueryOptimizer.simplifyCondition(a), QueryOptimizer.simplifyCondition(b))
  }

  def transform(tbc: TimeBoundedCondition, transform: TransformCondition): TimeBoundedCondition = {
    transform match {
      case Replace(from, to) =>
        val filtered = tbc.conditions.filterNot { c =>
          from.contains(c) || c == to
        }
        if (filtered.size != tbc.conditions.size)
          tbc.copy(conditions = filtered :+ to)
        else
          tbc
      case Original(_) =>
        // TODO: looks like, no need to do anything with 'other' conditions
        tbc
    }
  }
}
