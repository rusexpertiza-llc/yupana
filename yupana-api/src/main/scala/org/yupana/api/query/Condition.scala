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

import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.types.BinaryOperation

object Condition {
  def and(conditions: Seq[Condition]): Condition = {
    val nonEmpty = conditions.filterNot(_ == ConstantExpr(true))
    if (nonEmpty.size == 1) {
      nonEmpty.head
    } else if (nonEmpty.nonEmpty) {
      AndExpr(nonEmpty)
    } else {
      ConstantExpr(true)
    }
  }

  def or(conditions: Seq[Condition]): Condition = {
    val nonEmpty = conditions.filterNot(_ == ConstantExpr(true))
    if (nonEmpty.size == 1) {
      nonEmpty.head
    } else if (nonEmpty.nonEmpty) {
      OrExpr(nonEmpty)
    } else {
      ConstantExpr(true)
    }
  }

  def timeAndCondition(
      from: Expression.Aux[Time],
      to: Expression.Aux[Time],
      condition: Option[Condition]
  ): Condition = {
    AndExpr(
      Seq(
        BinaryOperationExpr(BinaryOperation.ge[Time], TimeExpr, from),
        BinaryOperationExpr(BinaryOperation.lt[Time], TimeExpr, to)
      ) ++ condition
    )
  }
}
