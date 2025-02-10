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
import org.yupana.api.types.Num

trait UnaryOperationSyntax {
  def truncYear(e: Expression[Time]) = TruncYearExpr(e)
  def truncQuarter(e: Expression[Time]) = TruncQuarterExpr(e)
  def truncMonth(e: Expression[Time]) = TruncMonthExpr(e)
  def truncDay(e: Expression[Time]) = TruncDayExpr(e)
  def truncHour(e: Expression[Time]) = TruncHourExpr(e)
  def truncMinute(e: Expression[Time]) = TruncMinuteExpr(e)
  def truncSecond(e: Expression[Time]) = TruncSecondExpr(e)
  def truncWeek(e: Expression[Time]) = TruncWeekExpr(e)

  def extractYear(e: Expression[Time]) = ExtractYearExpr(e)
  def extractQuarter(e: Expression[Time]) = ExtractQuarterExpr(e)
  def extractMonth(e: Expression[Time]) = ExtractMonthExpr(e)
  def extractDay(e: Expression[Time]) = ExtractDayExpr(e)
  def extractHour(e: Expression[Time]) = ExtractHourExpr(e)
  def extractMinute(e: Expression[Time]) = ExtractMinuteExpr(e)
  def extractSecond(e: Expression[Time]) = ExtractSecondExpr(e)

  def length(e: Expression[String]) = LengthExpr(e)
  def lower(e: Expression[String]) = LowerExpr(e)
  def upper(e: Expression[String]) = UpperExpr(e)
  def tokens(e: Expression[String]) = TokensExpr(e)
  def tokenizeArray(e: Expression[Seq[String]]) = ArrayTokensExpr(e)
  def split(e: Expression[String]) = SplitExpr(e)
  def arrayToString[T](e: Expression[Seq[T]]) = ArrayToStringExpr(e)
  def arrayLength[T](e: Expression[Seq[T]]) = ArrayLengthExpr(e)

  def not(e: Expression[Boolean]) = NotExpr(e)

  def abs[T](e: Expression[T])(implicit n: Num[T]) = AbsExpr(e)
  def minus[T](e: Expression[T])(implicit n: Num[T]) = UnaryMinusExpr(e)

  def isNull[T](e: Expression[T]) = IsNullExpr(e)
  def isNotNull[T](e: Expression[T]) = IsNotNullExpr(e)

  def lag[T](e: Expression[T]) = LagExpr(e)
}

object UnaryOperationSyntax extends UnaryOperationSyntax
