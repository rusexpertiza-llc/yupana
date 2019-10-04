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

import org.yupana.api.query._

object ConditionUtils {
  def simplify(condition: Condition): Condition = {
    condition match {
      case And(cs) => Condition.and(cs.flatMap(optimizeAnd))
      case Or(cs)  => Condition.or(cs.flatMap(optimizeOr))
      case c       => c
    }
  }

  def flatMap(c: Condition)(f: Condition => Condition): Condition = {
    def doFlat(xs: Seq[Condition]): Seq[Condition] = {
      xs.flatMap(
        x =>
          flatMap(x)(f) match {
            case EmptyCondition => None
            case nonEmpty       => Some(nonEmpty)
          }
      )
    }

    c match {
      case And(cs) => Condition.and(doFlat(cs))
      case Or(cs)  => Condition.or(doFlat(cs))
      case x       => f(x)
    }
  }

  def merge(a: Condition, b: Condition): Condition = {
    (a, b) match {
      case (EmptyCondition, x) => x
      case (x, EmptyCondition) => x
      case (And(as), And(bs))  => And((as ++ bs).distinct)
      case (_, Or(_))          => throw new IllegalArgumentException("OR is not supported yet")
      case (Or(_), _)          => throw new IllegalArgumentException("OR is not supported yet")
      case (And(as), _)        => And((as :+ b).distinct)
      case (_, And(bs))        => And((a +: bs).distinct)
      case _                   => And(Seq(a, b))
    }
  }

  def split(c: Condition)(p: Condition => Boolean): (Condition, Condition) = {
    def doSplit(c: Condition): (Condition, Condition) = {
      c match {
        case And(cs) =>
          val (a, b) = cs.map(doSplit).unzip
          (And(a), And(b))

        case Or(cs) =>
          val (a, b) = cs.map(doSplit).unzip
          (Or(a), Or(b))

        case x => if (p(x)) (x, EmptyCondition) else (EmptyCondition, x)
      }
    }

    val (a, b) = doSplit(c)

    (simplify(a), simplify(b))
  }

  private def optimizeAnd(c: Condition): Seq[Condition] = {
    c match {
      case And(cs) => cs.flatMap(optimizeAnd)
      case x       => Seq(simplify(x))
    }
  }

  private def optimizeOr(c: Condition): Seq[Condition] = {
    c match {
      case Or(cs) => cs.flatMap(optimizeOr)
      case x      => Seq(simplify(x))
    }
  }
}
