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

import org.yupana.api.query._

trait BinaryOperationSyntax {
  def minus[T](a: Expression.Aux[T], b: Expression.Aux[T])(implicit n: Numeric[T]) = MinusExpr(a, b)
  def plus[T](a: Expression.Aux[T], b: Expression.Aux[T])(implicit n: Numeric[T]) = PlusExpr(a, b)
  def times[T](a: Expression.Aux[T], b: Expression.Aux[T])(implicit n: Numeric[T]) = TimesExpr(a, b)
  def divInt[T](a: Expression.Aux[T], b: Expression.Aux[T])(implicit n: Integral[T]) = DivIntExpr(a, b)
  def divFrac[T](a: Expression.Aux[T], b: Expression.Aux[T])(implicit n: Fractional[T]) = DivFracExpr(a, b)

  def contains[T](a: Expression.Aux[Array[T]], b: Expression.Aux[T]) = ContainsExpr(a, b)
  def containsAll[T](a: Expression.Aux[Array[T]], b: Expression.Aux[Array[T]]) = ContainsAllExpr(a, b)
  def containsAny[T](a: Expression.Aux[Array[T]], b: Expression.Aux[Array[T]]) = ContainsAnyExpr(a, b)
}

object BinaryOperationSyntax extends BinaryOperationSyntax
