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

package org.yupana.core.utils

import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query.{ DimensionExpr, Expression, LinkExpr, MetricExpr }
import org.yupana.api.schema.{ Dimension, Metric }
import org.yupana.core.ConstantCalculator

object QueryUtils {
  def requiredMetrics(e: Expression[_]): Set[Metric] = {
    e.fold(Set.empty[Metric]) {
      case (s, MetricExpr(m)) => s + m
      case (s, _)             => s
    }
  }

  def requiredDimensions(e: Expression[_]): Set[Dimension] = {
    e.fold(Set.empty[Dimension]) {
      case (s, DimensionExpr(d)) => s + d
      case (s, LinkExpr(l, _))   => s + l.dimension
      case (s, _)                => s
    }
  }

  def requiredLinks(e: Expression[_]): Set[LinkExpr[_]] = {
    e.fold(Set.empty[LinkExpr[_]]) {
      case (s, l: LinkExpr[_]) => s + l
      case (s, _)              => s
    }
  }

  def getFromTo(filterOpt: Option[Condition], constantCalculator: ConstantCalculator): (Time, Time) = {
    filterOpt match {
      case Some(filter) =>
        val tbc = TimeBoundedCondition.single(constantCalculator, filter)
        val from = tbc.from.getOrElse(throw new IllegalArgumentException("FROM time is not defined"))
        val to = tbc.to.getOrElse(throw new IllegalArgumentException("TO time is not defined"))
        Time(from) -> Time(to)
      case None =>
        throw new IllegalArgumentException("Filter cannot be empty!")
    }
  }

}
