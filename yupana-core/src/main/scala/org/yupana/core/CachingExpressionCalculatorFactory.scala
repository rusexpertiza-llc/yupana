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
import org.yupana.api.query.{ Expression, Query }
import org.yupana.core.cache.{ Cache, CacheFactory }

class CachingExpressionCalculatorFactory(innerFactory: ExpressionCalculatorFactory)
    extends ExpressionCalculatorFactory {

  private val calculatorCache: Cache[String, (ExpressionCalculator, Map[Expression[_], Int])] =
    CacheFactory.initCache("calculator_cache")

  override def makeCalculator(
      query: Query,
      condition: Option[Condition]
  ): (ExpressionCalculator, Map[Expression[_], Int]) = {

    val key = makeKey(query)

    calculatorCache.caching(key) {
      innerFactory.makeCalculator(query, condition)
    }
  }

  private def makeKey(query: Query): String = {
    val fieldKey = query.fields.map(_.expr.makeKey).mkString(";")
    val filterKey = query.filter.map(_.makeKey)
    val postFilterKey = query.postFilter.map(_.makeKey)
    val groupingKey = query.groupBy.map(_.makeKey)
    s"${query.table}_${fieldKey}_${filterKey}_${postFilterKey}_${groupingKey}_${query.limit}"
  }
}
