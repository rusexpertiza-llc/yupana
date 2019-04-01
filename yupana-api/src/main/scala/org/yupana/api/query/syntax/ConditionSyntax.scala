package org.yupana.api.query.syntax

import org.yupana.api.query._
import org.yupana.api.types.{BinaryOperation, UnaryOperation}

trait ConditionSyntax {
  def in[T](e: Expression.Aux[T], consts: Set[T]) = In(e, consts)
  def notIn[T](e: Expression.Aux[T], consts: Set[T]) = NotIn(e, consts)

  def and(exprs: Condition*) = And(Seq(exprs:_*))
  def or(exprs: Condition*) = Or(Seq(exprs:_*))

  def gt[T : Ordering](left: Expression.Aux[T], right: Expression.Aux[T]) = SimpleCondition(BinaryOperationExpr(BinaryOperation.gt, left, right))
  def lt[T : Ordering](left: Expression.Aux[T], right: Expression.Aux[T]) = SimpleCondition(BinaryOperationExpr(BinaryOperation.lt, left, right))
  def ge[T : Ordering](left: Expression.Aux[T], right: Expression.Aux[T]) = SimpleCondition(BinaryOperationExpr(BinaryOperation.ge, left, right))
  def le[T : Ordering](left: Expression.Aux[T], right: Expression.Aux[T]) = SimpleCondition(BinaryOperationExpr(BinaryOperation.le, left, right))
  def equ[T : Ordering](left: Expression.Aux[T], right: Expression.Aux[T]) = SimpleCondition(BinaryOperationExpr(BinaryOperation.equ, left, right))
  def neq[T : Ordering](left: Expression.Aux[T], right: Expression.Aux[T]) = SimpleCondition(BinaryOperationExpr(BinaryOperation.neq, left, right))

  def expr(e: Expression.Aux[Boolean]) = SimpleCondition(e)

  def isNull[T](e: Expression.Aux[T]) = SimpleCondition(UnaryOperationExpr(UnaryOperation.isNull[T], e))
  def isNotNull[T](e: Expression.Aux[T]) = SimpleCondition(UnaryOperationExpr(UnaryOperation.isNotNull[T], e))
}

object ConditionSyntax extends ConditionSyntax
