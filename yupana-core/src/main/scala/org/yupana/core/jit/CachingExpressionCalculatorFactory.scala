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

package org.yupana.core.jit

import org.yupana.api.query.Expression.Condition
import org.yupana.api.query.Query
import org.yupana.api.utils.Tokenizer
import org.yupana.cache.{ Cache, CacheFactory }
import org.yupana.core.model.DatasetSchema

object CachingExpressionCalculatorFactory extends ExpressionCalculatorFactory {

  private val calculatorCache: Cache[Query, (ExpressionCalculator, DatasetSchema)] =
    CacheFactory.initCache("calculator_cache")

  override def makeCalculator(
      query: Query,
      condition: Option[Condition],
      tokenizer: Tokenizer
  ): (ExpressionCalculator, DatasetSchema) = {

    calculatorCache.caching(query) {
      JIT.makeCalculator(query, condition, tokenizer)
    }
  }
}
