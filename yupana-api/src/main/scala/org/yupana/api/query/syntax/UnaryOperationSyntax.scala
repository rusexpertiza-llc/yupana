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

package org.yupana.api.query.syntax

import org.yupana.api.Time
import org.yupana.api.query._

trait UnaryOperationSyntax {
  def truncYear(e: Expression.Aux[Time]) = TruncYearExpr(e)
  def truncMonth(e: Expression.Aux[Time]) = TruncMonthExpr(e)
  def truncDay(e: Expression.Aux[Time]) = TruncDayExpr(e)
  def truncHour(e: Expression.Aux[Time]) = TruncHourExpr(e)
  def truncMinute(e: Expression.Aux[Time]) = TruncMinuteExpr(e)
  def truncSecond(e: Expression.Aux[Time]) = TruncSecondExpr(e)
  def truncWeek(e: Expression.Aux[Time]) = TruncWeekExpr(e)

  def extractYear(e: Expression.Aux[Time]) = ExtractYearExpr(e)
  def extractMonth(e: Expression.Aux[Time]) = ExtractMonthExpr(e)
  def extractDay(e: Expression.Aux[Time]) = ExtractDayExpr(e)
  def extractHour(e: Expression.Aux[Time]) = ExtractHourExpr(e)
  def extractMinute(e: Expression.Aux[Time]) = ExtractMinuteExpr(e)
  def extractSecond(e: Expression.Aux[Time]) = ExtractSecondExpr(e)

  def length(e: Expression.Aux[String]) = LengthExpr(e)
  def lower(e: Expression.Aux[String]) = LowerExpr(e)
  def upper(e: Expression.Aux[String]) = UpperExpr(e)
  def tokens(e: Expression.Aux[String]) = TokensExpr(e)
  def tokenizeArray(e: Expression.Aux[Array[String]]) = ArrayTokensExpr(e)
  def split(e: Expression.Aux[String]) = SplitExpr(e)
  def arrayToString[T](e: Expression.Aux[Array[T]]) = ArrayToStringExpr(e)

  def not(e: Expression.Aux[Boolean]) = NotExpr(e)

  def abs[T](e: Expression.Aux[T])(implicit n: Numeric[T]) = AbsExpr(e)
  def minus[T](e: Expression.Aux[T])(implicit n: Numeric[T]) = UnaryMinusExpr(e)

  def isNull[T](e: Expression.Aux[T]) = IsNullExpr(e)
  def isNotNull[T](e: Expression.Aux[T]) = IsNotNullExpr(e)

  def lag[T](e: Expression.Aux[T]) = LagExpr(e)
}

object UnaryOperationSyntax extends UnaryOperationSyntax
