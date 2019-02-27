package org.yupana.api.query.syntax

import org.yupana.api.query._
import org.yupana.api.types.Comparison

trait ConditionSyntax {
  def in[T](e: Expression.Aux[T], consts: Set[T]) = In(e, consts)
  def notIn[T](e: Expression.Aux[T], consts: Set[T]) = NotIn(e, consts)

  def and(exprs: Condition*) = And(Seq(exprs:_*))
  def or(exprs: Condition*) = Or(Seq(exprs:_*))

  def gt[T : Ordering](left: Expression.Aux[T], right: Expression.Aux[T]) = Compare(Comparison.gt, left, right)
  def lt[T : Ordering](left: Expression.Aux[T], right: Expression.Aux[T]) = Compare(Comparison.lt, left, right)
  def ge[T : Ordering](left: Expression.Aux[T], right: Expression.Aux[T]) = Compare(Comparison.ge, left, right)
  def le[T : Ordering](left: Expression.Aux[T], right: Expression.Aux[T]) = Compare(Comparison.le, left, right)
  def equ[T : Ordering](left: Expression.Aux[T], right: Expression.Aux[T]) = Compare(Comparison.eq, left, right)
  def neq[T : Ordering](left: Expression.Aux[T], right: Expression.Aux[T]) = Compare(Comparison.ne, left, right)

  def isNull[T](e: Expression.Aux[T]) = IsNull(e)
  def isNotNull[T](e: Expression.Aux[T]) = IsNotNull(e)
}

object ConditionSyntax extends ConditionSyntax
