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

import scala.collection.mutable

class QueryContext(
    val query: Query,
    val exprsIndex: mutable.HashMap[Expression[_], Int],
    val postCondition: Option[Condition],
    val calculator: ExpressionCalculator
) extends Serializable {
  val groupByIndices: Array[Int] = query.groupBy.map(exprsIndex.apply).toArray
  val linkExprs: Seq[LinkExpr[_]] = exprsIndex.keys.collect { case le: LinkExpr[_] => le }.toSeq
}

object QueryContext {
  def apply(query: Query, postCondition: Option[Condition]): QueryContext = {
    val (calculator, index) = ExpressionCalculator.makeCalculator(query, postCondition)
    new QueryContext(query, mutable.HashMap(index.toSeq: _*), postCondition, calculator)
  }
}
