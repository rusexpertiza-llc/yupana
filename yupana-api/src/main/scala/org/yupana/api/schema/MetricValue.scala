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

package org.yupana.api.schema

/**
  * Metric and it's value
  */
trait MetricValue extends Serializable {
  val metric: Metric
  val value: metric.T

  override def toString: String = s"MetricValue(${metric.name}, $value)"

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: MetricValue => this.metric == that.metric && this.value == that.value
      case _                 => false
    }
  }

  override def hashCode(): Int = {
    31 * (31 + metric.hashCode()) + value.hashCode()
  }
}

object MetricValue {
  def apply[T](m: Metric.Aux[T], v: T): MetricValue = new MetricValue {
    val metric: Metric.Aux[T] = m
    val value: T = v
  }
}
