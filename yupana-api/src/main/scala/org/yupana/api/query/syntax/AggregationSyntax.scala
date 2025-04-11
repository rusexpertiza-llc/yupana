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
import org.yupana.api.schema.Metric
import org.yupana.api.types.DataType
import org.yupana.api.types.guards.PlusGuard

trait AggregationSyntax {

  def sum[In, Out](e: Expression[In])(
      implicit guard: SumExpr.SumGuard[In, Out],
      plus: PlusGuard[Out, Out, Out],
      dt: DataType.Aux[Out]
  ): SumExpr[In, Out] = {
    SumExpr(e)
  }
  def sum[In, Out](m: Metric.Aux[In])(
      implicit guard: SumExpr.SumGuard[In, Out],
      plus: PlusGuard[Out, Out, Out],
      dt: DataType.Aux[Out]
  ): SumExpr[In, Out] = {
    sum[In, Out](MetricExpr(m))
  }

  def min[T](e: Expression[T])(implicit ord: Ordering[T]) = MinExpr(e)
  def min[T](m: Metric.Aux[T])(implicit ord: Ordering[T]) = MinExpr(MetricExpr(m))
  def max[T](e: Expression[T])(implicit ord: Ordering[T]) = MaxExpr(e)
  def max[T](m: Metric.Aux[T])(implicit ord: Ordering[T]) = MaxExpr(MetricExpr(m))
  def count[T](e: Expression[T]) = CountExpr(e)
  def count[T](m: Metric.Aux[T]) = CountExpr(MetricExpr(m))
  def distinctCount[T](e: Expression[T]) = DistinctCountExpr(e)
  def hllCount[T](e: Expression[T], a: Double) = HLLCountExpr(e, a)
  def distinctRandom[T](e: Expression[T]) = DistinctRandomExpr(e)
}

object AggregationSyntax extends AggregationSyntax
