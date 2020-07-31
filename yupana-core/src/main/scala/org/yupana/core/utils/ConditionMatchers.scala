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
import org.yupana.api.query.{ BinaryOperationExpr, Expression, UnaryOperationExpr }
import org.yupana.api.types.UnaryOperation

object ConditionMatchers {

  object Equ {
    def unapply(condition: Condition): Option[(Expression, Expression)] = {
      condition match {
        case BinaryOperationExpr(op, a, b) if op.name == "==" => Some((a, b))
        case _                                                => None
      }
    }
  }

  object Lower {
    def unapply(expr: Expression): Option[Expression] = {
      expr match {
        case UnaryOperationExpr(f, e) if f.name == UnaryOperation.LOWER => Some(e)
        case _                                                          => None
      }
    }
  }

  object Neq {
    def unapply(condition: Condition): Option[(Expression, Expression)] = {
      condition match {
        case BinaryOperationExpr(op, a, b) if op.name == "!=" => Some((a, b))
        case _                                                => None
      }
    }
  }

  object Lt {
    def unapply(condition: Condition): Option[(Expression, Expression)] = {
      condition match {
        case BinaryOperationExpr(op, a, b) if op.name == "<" => Some((a, b))
        case _                                               => None
      }
    }
  }

  object Gt {
    def unapply(condition: Condition): Option[(Expression, Expression)] = {
      condition match {
        case BinaryOperationExpr(op, a, b) if op.name == ">" => Some((a, b))
        case _                                               => None
      }
    }
  }

  object Le {
    def unapply(condition: Condition): Option[(Expression, Expression)] = {
      condition match {
        case BinaryOperationExpr(op, a, b) if op.name == "<=" => Some((a, b))
        case _                                                => None
      }
    }
  }

  object Ge {
    def unapply(condition: Condition): Option[(Expression, Expression)] = {
      condition match {
        case BinaryOperationExpr(op, a, b) if op.name == ">=" => Some((a, b))
        case _                                                => None
      }
    }
  }
}
