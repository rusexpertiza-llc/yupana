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

import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.utils.Tokenizer
import org.yupana.core.jit.{ ExpressionCalculator, ExpressionCalculatorFactory }
import org.yupana.core.model.DatasetSchema
import org.yupana.core.utils.metric.MetricQueryCollector

class QueryContext(
    val query: Query,
    startTime: Time,
    params: IndexedSeq[Any],
    val postCondition: Option[Condition],
    tokenizer: Tokenizer,
    calculatorFactory: ExpressionCalculatorFactory,
    val metricCollector: MetricQueryCollector
) extends Serializable {

  @transient private var calc: ExpressionCalculator = _
  @transient private var schema: DatasetSchema = _

  def datasetSchema: DatasetSchema = {
    if (schema == null) init()
    schema
  }

  def calculator: ExpressionCalculator = {
    if (calc == null) init()
    calc
  }

  lazy val linkExprs: Seq[LinkExpr[_]] = datasetSchema.exprIndex.keys.collect { case le: LinkExpr[_] => le }.toSeq

  private def init(): Unit = {
    metricCollector.initQueryContext.measure(1) {
      val (calculator, dsSchema) = calculatorFactory.makeCalculator(query, startTime, params, postCondition, tokenizer)
      calc = calculator
      schema = dsSchema
    }
  }
}
