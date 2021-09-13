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

  def filter(c: Condition)(p: Condition => Boolean): Condition = {
    def doFilter(c: Condition): Condition = {
      c match {
        case AndExpr(cs) =>
          val a = cs.map(doFilter)
          AndExpr(a)

        case OrExpr(cs) =>
          val a = cs.map(doFilter)
          OrExpr(a)

        case x => if (p(x)) x else ConstantExpr(true)
      }
    }

    val a = doFilter(c)

    QueryOptimizer.simplifyCondition(a)
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
        //TODO: looks like, no need to do anything with 'other' conditions
        tbc
    }
  }
}
