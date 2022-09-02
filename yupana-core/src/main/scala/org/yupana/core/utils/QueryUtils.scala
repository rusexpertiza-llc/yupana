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
import org.yupana.api.query.{ Expression, MetricExpr }
import org.yupana.api.schema.Metric

object QueryUtils {
  def requiredMetrics(e: Expression[_]): Set[Metric] = {
    e.fold(Set.empty[Metric]) {
      case (s, MetricExpr(m)) => s + m
      case (s, _)             => s
    }
  }

  def getFromTo(tbc: TimeBoundedCondition): (Time, Time) = {
    (tbc.from, tbc.to) match {
      case (Some(from), Some(to)) => Time(from) -> Time(to)
      case (Some(_), None)        => throw new IllegalArgumentException(s"TO time is not defined in ${tbc.toCondition}")
      case (None, Some(_)) => throw new IllegalArgumentException(s"FROM time is not defined in ${tbc.toCondition}")
      case (None, None)    => throw new IllegalArgumentException(s"time interval is not defined in ${tbc.toCondition}")
    }
  }

}
