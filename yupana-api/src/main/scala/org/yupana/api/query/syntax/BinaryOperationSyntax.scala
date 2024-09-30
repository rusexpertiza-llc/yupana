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
import org.yupana.api.types.guards.{ DivGuard, MinusGuard, PlusGuard, TimesGuard }

trait BinaryOperationSyntax {
  def minus[A, B, R](a: Expression[A], b: Expression[B])(implicit g: MinusGuard[A, B, R]) = MinusExpr(a, b)
  def plus[A, B, R](a: Expression[A], b: Expression[B])(implicit g: PlusGuard[A, B, R]) =
    PlusExpr[A, B, R](a, b)
  def times[A, B, R](a: Expression[A], b: Expression[B])(implicit tg: TimesGuard[A, B, R]) = TimesExpr(a, b)
  def div[A, B, R](a: Expression[A], b: Expression[B])(implicit dg: DivGuard[A, B, R]) = DivExpr(a, b)

  def contains[T](a: Expression[Seq[T]], b: Expression[T]) = ContainsExpr(a, b)
  def containsAll[T](a: Expression[Seq[T]], b: Expression[Seq[T]]) = ContainsAllExpr(a, b)
  def containsAny[T](a: Expression[Seq[T]], b: Expression[Seq[T]]) = ContainsAnyExpr(a, b)
  def containsSame[T](a: Expression[Seq[T]], b: Expression[Seq[T]]) = ContainsSameExpr(a, b)
}

object BinaryOperationSyntax extends BinaryOperationSyntax
