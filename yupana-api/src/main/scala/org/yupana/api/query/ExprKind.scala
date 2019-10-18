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

package org.yupana.api.query

sealed trait ExprKind
case object Const extends ExprKind
case object Simple extends ExprKind
case object Aggregate extends ExprKind
case object Window extends ExprKind
case object Invalid extends ExprKind

object ExprKind {
  def combine(a: ExprKind, b: ExprKind): ExprKind = {
    (a, b) match {
      case (x, y) if x == y => x
      case (Invalid, _)     => Invalid
      case (_, Invalid)     => Invalid
      case (x, Const)       => x
      case (Const, x)       => x
      case (Window, Simple) => Window
      case (Simple, Window) => Window
      case _                => Invalid
    }
  }
}
