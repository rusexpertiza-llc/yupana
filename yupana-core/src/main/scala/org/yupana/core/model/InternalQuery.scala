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

package org.yupana.core.model

import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query.{ Expression, QueryHint }
import org.yupana.api.schema.Table
import org.yupana.core.ConstantCalculator
import org.yupana.core.utils.FlatAndCondition

case class InternalQuery(
    table: Table,
    exprs: Set[Expression[_]],
    condition: Seq[FlatAndCondition],
    hints: Seq[QueryHint] = Seq.empty
)

object InternalQuery {
  def apply(
      table: Table,
      exprs: Set[Expression[_]],
      condition: Condition,
      startTime: Time,
      params: Array[Any],
      hints: Seq[QueryHint]
  )(implicit calculator: ConstantCalculator): InternalQuery =
    new InternalQuery(table, exprs, FlatAndCondition(calculator, condition, startTime, params), hints)

  def apply(
      table: Table,
      exprs: Set[Expression[_]],
      condition: Condition,
      startTime: Time,
      params: Array[Any]
  )(implicit calculator: ConstantCalculator): InternalQuery =
    new InternalQuery(table, exprs, FlatAndCondition(calculator, condition, startTime, params), Seq.empty)
}
