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

package org.yupana.core

import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.core.utils.metric.MetricQueryCollector

import scala.collection.mutable

class QueryContext(
    val query: Query,
    val postCondition: Option[Condition],
    calculatorFactory: ExpressionCalculatorFactory,
    val metricCollector: MetricQueryCollector
) extends Serializable {
  @transient private var calc: ExpressionCalculator = _
  @transient private var idx: mutable.Map[Expression[_], Int] = _

  def exprsIndex: mutable.Map[Expression[_], Int] = {
    if (idx == null) init()
    idx
  }
  def calculator: ExpressionCalculator = {
    if (calc == null) init()
    calc
  }

  lazy val groupByIndices: Array[Int] = query.groupBy.map(exprsIndex.apply).toArray
  lazy val linkExprs: Seq[LinkExpr[_]] = exprsIndex.keys.collect { case le: LinkExpr[_] => le }.toSeq

  private def init(): Unit = {
    metricCollector.initQueryContext.measure(1) {
      val (calculator, index) = calculatorFactory.makeCalculator(query, postCondition)
      calc = calculator
      idx = mutable.HashMap(index.toSeq: _*)
    }
  }
}
