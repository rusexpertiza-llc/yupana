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

package org.yupana.api.utils

import org.yupana.api.Time
import org.yupana.api.query._

object ConditionMatchers {

  // This is an ugly hack to allow pattern match on GADT
  trait EqMatcher[T] {
    def unapply(condition: Expression[_]): Option[(Expression[T], Expression[T])] = {
      condition match {
        case EqExpr(a, b) => Some((a.asInstanceOf[Expression[T]], b.asInstanceOf[Expression[T]]))
        case _            => None
      }
    }
  }

  trait NeqMatcher[T] {
    def unapply(condition: Expression[_]): Option[(Expression[T], Expression[T])] = {
      condition match {
        case NeqExpr(a, b) => Some((a.asInstanceOf[Expression[T]], b.asInstanceOf[Expression[T]]))
        case _             => None
      }
    }
  }

  trait GtMatcher[T] {
    def unapply(condition: SimpleCondition): Option[(Expression[T], Expression[T])] = {
      condition match {
        case GtExpr(a, b) => Some((a.asInstanceOf[Expression[T]], b.asInstanceOf[Expression[T]]))
        case _            => None
      }
    }
  }

  trait LtMatcher[T] {
    def unapply(condition: SimpleCondition): Option[(Expression[T], Expression[T])] = {
      condition match {
        case LtExpr(a, b) => Some((a.asInstanceOf[Expression[T]], b.asInstanceOf[Expression[T]]))
        case _            => None
      }
    }
  }

  trait GeMatcher[T] {
    def unapply(condition: SimpleCondition): Option[(Expression[T], Expression[T])] = {
      condition match {
        case GeExpr(a, b) => Some((a.asInstanceOf[Expression[T]], b.asInstanceOf[Expression[T]]))
        case _            => None
      }
    }
  }

  trait LeMatcher[T] {
    def unapply(condition: SimpleCondition): Option[(Expression[T], Expression[T])] = {
      condition match {
        case LeExpr(a, b) => Some((a.asInstanceOf[Expression[T]], b.asInstanceOf[Expression[T]]))
        case _            => None
      }
    }
  }

  trait InMatcher[T] {
    def unapply(condition: SimpleCondition): Option[(Expression[T], Set[ValueExpr[T]])] = {
      condition match {
        case InExpr(a, b) => Some((a.asInstanceOf[Expression[T]], b.asInstanceOf[Set[ValueExpr[T]]]))
        case _            => None
      }
    }
  }

  trait NotInMatcher[T] {
    def unapply(condition: SimpleCondition): Option[(Expression[T], Set[ValueExpr[T]])] = {
      condition match {
        case NotInExpr(a, b) => Some((a.asInstanceOf[Expression[T]], b.asInstanceOf[Set[ValueExpr[T]]]))
        case _               => None
      }
    }
  }

  // This is a hack used to avoid smashing the stack with tuples (can't explain why)
  object EqUntyped {
    def unapply(e: Expression[_]): Option[(Expression[_], Expression[_])] = {
      e match {
        case EqExpr(a, b) => Some((a, b))
        case _            => None
      }
    }
  }

  object InUntyped {
    def unapply(e: Expression[_]): Option[(Expression[_], Set[ValueExpr[Any]])] = {
      e match {
        case InExpr(t: Expression[_], v) => Some((t, v))
        case _                           => None
      }
    }
  }

  object EqString extends EqMatcher[String]
  object NeqString extends NeqMatcher[String]
  object InString extends InMatcher[String]
  object NotInString extends NotInMatcher[String]

  object EqTime extends EqMatcher[Time]
  object NeqTime extends NeqMatcher[Time]
  object GtTime extends GtMatcher[Time]
  object LtTime extends LtMatcher[Time]
  object GeTime extends GeMatcher[Time]
  object LeTime extends LeMatcher[Time]
  object InTime extends InMatcher[Time]
  object NotInTime extends NotInMatcher[Time]
}
