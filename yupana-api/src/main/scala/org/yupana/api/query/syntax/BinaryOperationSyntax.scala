package org.yupana.api.query.syntax

import org.yupana.api.query.{BinaryOperationExpr, Expression}
import org.yupana.api.types.{BinaryOperation, DataType}

trait BinaryOperationSyntax {
  def minus[T](a: Expression.Aux[T], b: Expression.Aux[T])(implicit n: Numeric[T], dt: DataType.Aux[T]) = BinaryOperationExpr(BinaryOperation.minus(dt), a, b)
  def plus[T](a: Expression.Aux[T], b: Expression.Aux[T])(implicit n: Numeric[T], dt: DataType.Aux[T]) = BinaryOperationExpr(BinaryOperation.plus(dt), a, b)
  def times[T](a: Expression.Aux[T], b: Expression.Aux[T])(implicit n: Numeric[T], dt: DataType.Aux[T]) = BinaryOperationExpr(BinaryOperation.multiply(dt), a, b)
  def divInt[T](a: Expression.Aux[T], b: Expression.Aux[T])(implicit n: Integral[T], dt: DataType.Aux[T]) = BinaryOperationExpr(BinaryOperation.divideInt(dt), a, b)
  def divFrac[T](a: Expression.Aux[T], b: Expression.Aux[T])(implicit n: Fractional[T], dt: DataType.Aux[T]) = BinaryOperationExpr(BinaryOperation.divideFrac(dt), a, b)

  def contains[T](a: Expression.Aux[Array[T]], b: Expression.Aux[T]) = BinaryOperationExpr(BinaryOperation.contains[T], a, b)
  def containsAll[T](a: Expression.Aux[Array[T]], b: Expression.Aux[Array[T]]) = BinaryOperationExpr(BinaryOperation.containsAll[T], a, b)
  def containsAny[T](a: Expression.Aux[Array[T]], b: Expression.Aux[Array[T]]) = BinaryOperationExpr(BinaryOperation.containsAny[T], a, b)
}

object BinaryOperationSyntax extends BinaryOperationSyntax
