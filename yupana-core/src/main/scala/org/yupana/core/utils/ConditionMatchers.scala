package org.yupana.core.utils

import org.yupana.api.query.{BinaryOperationExpr, Condition, Expression, SimpleCondition}

object ConditionMatchers {

  object Equ {
    def unapply(condition: Condition): Option[(Expression, Expression)] = {
      condition match {
        case SimpleCondition(BinaryOperationExpr(op, a, b)) if op.name == "==" => Some((a, b))
        case _ => None
      }
    }
  }

  object Neq {
    def unapply(condition: Condition): Option[(Expression, Expression)] = {
      condition match {
        case SimpleCondition(BinaryOperationExpr(op, a, b)) if op.name == "!=" => Some((a, b))
        case _ => None
      }
    }
  }

  object Lt {
    def unapply(condition: Condition): Option[(Expression, Expression)] = {
      condition match {
        case SimpleCondition(BinaryOperationExpr(op, a, b)) if op.name == "<" => Some((a, b))
        case _ => None
      }
    }
  }

  object Gt {
    def unapply(condition: Condition): Option[(Expression, Expression)] = {
      condition match {
        case SimpleCondition(BinaryOperationExpr(op, a, b)) if op.name == ">" => Some((a, b))
        case _ => None
      }
    }
  }

  object Le {
    def unapply(condition: Condition): Option[(Expression, Expression)] = {
      condition match {
        case SimpleCondition(BinaryOperationExpr(op, a, b)) if op.name == "<=" => Some((a, b))
        case _ => None
      }
    }
  }

  object Ge {
    def unapply(condition: Condition): Option[(Expression, Expression)] = {
      condition match {
        case SimpleCondition(BinaryOperationExpr(op, a, b)) if op.name == ">=" => Some((a, b))
        case _ => None
      }
    }
  }
}
