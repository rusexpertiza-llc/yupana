package org.yupana.api.query.syntax

import org.yupana.api.Time
import org.yupana.api.query.{Expression, UnaryOperationExpr}
import org.yupana.api.types.{DataType, UnaryOperation}

trait UnaryOperationSyntax {
  def truncYear(e: Expression.Aux[Time]) = UnaryOperationExpr(UnaryOperation.truncYear, e)
  def truncMonth(e: Expression.Aux[Time]) = UnaryOperationExpr(UnaryOperation.truncMonth, e)
  def truncDay(e: Expression.Aux[Time]) = UnaryOperationExpr(UnaryOperation.truncDay, e)
  def truncHour(e: Expression.Aux[Time]) = UnaryOperationExpr(UnaryOperation.truncHour, e)
  def truncMinute(e: Expression.Aux[Time]) = UnaryOperationExpr(UnaryOperation.truncMinute, e)
  def truncSecond(e: Expression.Aux[Time]) = UnaryOperationExpr(UnaryOperation.truncSecond, e)
  def truncWeek(e: Expression.Aux[Time]) = UnaryOperationExpr(UnaryOperation.truncWeek, e)

  def extractYear(e: Expression.Aux[Time]) = UnaryOperationExpr(UnaryOperation.extractYear, e)
  def extractMonth(e: Expression.Aux[Time]) = UnaryOperationExpr(UnaryOperation.extractMonth, e)
  def extractDay(e: Expression.Aux[Time]) = UnaryOperationExpr(UnaryOperation.extractDay, e)
  def extractHour(e: Expression.Aux[Time]) = UnaryOperationExpr(UnaryOperation.extractHour, e)
  def extractMinute(e: Expression.Aux[Time]) = UnaryOperationExpr(UnaryOperation.extractMinute, e)
  def extractSecond(e: Expression.Aux[Time]) = UnaryOperationExpr(UnaryOperation.extractSecond, e)

  def length(e: Expression.Aux[String]) = UnaryOperationExpr(UnaryOperation.length, e)
  def stem(e: Expression.Aux[String]) = UnaryOperationExpr(UnaryOperation.stem, e)
  def split(e: Expression.Aux[String]) = UnaryOperationExpr(UnaryOperation.splitString, e)

  def not(e: Expression.Aux[Boolean]) = UnaryOperationExpr(UnaryOperation.not, e)

  def abs[T](e: Expression.Aux[T])(implicit n: Numeric[T], dt: DataType.Aux[T]) = UnaryOperationExpr(UnaryOperation.abs, e)
  def minus[T](e: Expression.Aux[T])(implicit n: Numeric[T], dt: DataType.Aux[T]) = UnaryOperationExpr(UnaryOperation.minus, e)
}

object UnaryOperationSyntax extends UnaryOperationSyntax
