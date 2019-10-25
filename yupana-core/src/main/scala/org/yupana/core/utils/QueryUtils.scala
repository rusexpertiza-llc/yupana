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

import org.yupana.api.query.{ DimensionExpr, Expression, LinkExpr, MetricExpr }
import org.yupana.api.schema.{ Dimension, Metric }

object QueryUtils {
  def requiredMetrics(e: Expression): Set[Metric] = {
    e.fold(Set.empty[Metric]) {
      case (s, MetricExpr(m)) => s + m
      case (s, _)             => s
    }
  }

  def requiredDimensions(e: Expression): Set[Dimension] = {
    e.fold(Set.empty[Dimension]) {
      case (s, DimensionExpr(d)) => s + d
      case (s, LinkExpr(l, _))   => s + l.dimension
      case (s, _)                => s
    }
  }

  def requiredLinks(e: Expression): Set[LinkExpr] = {
    e.fold(Set.empty[LinkExpr]) {
      case (s, l: LinkExpr) => s + l
      case (s, _)           => s
    }
  }

}
