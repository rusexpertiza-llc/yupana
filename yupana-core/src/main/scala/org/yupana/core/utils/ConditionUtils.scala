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
          case TrueExpr           => None
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

  def transform(fac: FlatAndCondition, transformations: Seq[ConditionTransformation]): FlatAndCondition = {
    val (adds, removes) = transformations.partitionMap {
      case a: AddCondition    => Left(a)
      case r: RemoveCondition => Right(r)
    }
    val afterRemove = removes.foldLeft(fac)((c, t) => c.copy(conditions = c.conditions.filterNot(_ == t.c)))

    adds.foldLeft(afterRemove)((c, t) =>
      if (!c.conditions.contains(t.c))
        c.copy(conditions = t.c +: c.conditions)
      else c
    )
  }

  def value[T](v: ValueExpr[T]): T = {
    v match {
      case ConstantExpr(t)      => t
      case NullExpr(_)          => null.asInstanceOf[T]
      case TrueExpr             => true
      case FalseExpr            => false
      case TupleValueExpr(a, b) => (value(a), value(b))
      case x                    => throw new IllegalStateException(s"Unexpected value $x in DAO")
    }
  }
}
