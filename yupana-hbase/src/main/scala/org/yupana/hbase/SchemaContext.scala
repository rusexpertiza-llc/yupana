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

package org.yupana.hbase

import org.yupana.api.query.DimensionExpr
import org.yupana.core.model.InternalQuery
import org.yupana.api.schema.{Dimension, Metric}

import scala.collection.mutable

case class SchemaContext(query: InternalQuery,
                         fieldIndexMap: mutable.HashMap[Byte, Metric],
                         tagIndexMap: mutable.Map[Dimension, Int],
                         requiredTags: Set[Dimension]
                        )

object SchemaContext {
  def apply(query: InternalQuery): SchemaContext = {
    val fieldIndexMap = mutable.HashMap(query.table.metrics.map(f => f.tag -> f): _*)

    val tagIndexMap = mutable.HashMap(query.table.dimensionSeq.zipWithIndex: _*)

    val requiredTags = query.exprs.collect {
      case DimensionExpr(tag) => tag
    }

    new SchemaContext(query, fieldIndexMap, tagIndexMap, requiredTags)
  }
}
