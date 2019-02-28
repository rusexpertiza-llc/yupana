package org.yupana.api.query.syntax

import org.yupana.api.Time
import org.yupana.api.query.{Expression, FunctionExpr}
import org.yupana.api.types.{DataType, UnaryOperation}

trait UnaryOperationSyntax {
  def truncYear(e: Expression.Aux[Time]) = FunctionExpr(UnaryOperation.truncYear, e)
  def truncMonth(e: Expression.Aux[Time]) = FunctionExpr(UnaryOperation.truncMonth, e)
  def truncDay(e: Expression.Aux[Time]) = FunctionExpr(UnaryOperation.truncDay, e)
  def truncHour(e: Expression.Aux[Time]) = FunctionExpr(UnaryOperation.truncHour, e)
  def truncMinute(e: Expression.Aux[Time]) = FunctionExpr(UnaryOperation.truncMinute, e)
  def truncSecond(e: Expression.Aux[Time]) = FunctionExpr(UnaryOperation.truncSecond, e)
  def truncWeek(e: Expression.Aux[Time]) = FunctionExpr(UnaryOperation.truncWeek, e)

  def extractYear(e: Expression.Aux[Time]) = FunctionExpr(UnaryOperation.extractYear, e)
  def extractMonth(e: Expression.Aux[Time]) = FunctionExpr(UnaryOperation.extractMonth, e)
  def extractDay(e: Expression.Aux[Time]) = FunctionExpr(UnaryOperation.extractDay, e)
  def extractHour(e: Expression.Aux[Time]) = FunctionExpr(UnaryOperation.extractHour, e)
  def extractMinute(e: Expression.Aux[Time]) = FunctionExpr(UnaryOperation.extractMinute, e)
  def extractSecond(e: Expression.Aux[Time]) = FunctionExpr(UnaryOperation.extractSecond, e)

  def length(e: Expression.Aux[String]) = FunctionExpr(UnaryOperation.length, e)

  def abs[T](e: Expression.Aux[T])(implicit n: Numeric[T], dt: DataType.Aux[T]) = FunctionExpr(UnaryOperation.abs(dt), e)
}

object UnaryOperationSyntax extends UnaryOperationSyntax
