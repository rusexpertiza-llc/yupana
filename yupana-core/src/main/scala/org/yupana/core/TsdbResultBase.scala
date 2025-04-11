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

import org.yupana.api.query.Expression
import org.yupana.core.model.BatchDataset

trait TsdbResultBase[T[_]] {

  protected val nameIndex: Seq[(String, Int)] =
    queryContext.query.fields.map(f => f.name -> queryContext.datasetSchema.fieldIndex(f.expr))
  protected lazy val nameIndexMap: Map[String, Int] = nameIndex.toMap
  protected lazy val fieldIndex: Array[Int] = nameIndex.map(_._2).toArray
  protected lazy val exprByIndex: Map[Int, Expression[_]] = queryContext.datasetSchema.exprIndex.map {
    case (expr, idx) => idx -> expr
  }

  def data: T[BatchDataset]

  def queryContext: QueryContext

  def dataIndexForFieldName(name: String): Int = nameIndexMap(name)

  def dataIndexForFieldIndex(idx: Int): Int = fieldIndex(idx)
}
