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

package org.yupana.externallinks

import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.schema.ExternalLink
import org.yupana.api.utils.SortedSetIterator
import org.yupana.core.{ Dictionary, TsdbBase }
import org.yupana.core.utils.{ SparseTable, Table }

abstract class DimIdBasedExternalLinkService[T <: ExternalLink](val tsdb: TsdbBase)
    extends SimpleExternalLinkConditionHandler[T]
    with SimpleExternalLinkValueExtractor[T] {

  lazy val dictionary: Dictionary = tsdb.dictionary(externalLink.dimension)

  def dimIdsForAllFieldsValues(fieldsValues: Seq[(String, Set[String])]): SortedSetIterator[Long]

  def dimIdsForAnyFieldsValues(fieldsValues: Seq[(String, Set[String])]): SortedSetIterator[Long]

  override def fieldValuesForDimValues(fields: Set[String], dimValues: Set[String]): Table[String, String, String] = {
    val ids = dictionary.findIdsByValues(dimValues).map(_.swap)
    if (ids.nonEmpty) {
      fieldValuesForDimIds(fields, ids.keySet).mapRowKeys(ids)
    } else {
      SparseTable.empty
    }
  }

  override def includeCondition(values: Seq[(String, Set[String])]): Condition = {
    val ids = dimIdsForAllFieldsValues(values)
    DimIdInExpr(new DimensionExpr(externalLink.dimension), ids)
  }

  override def excludeCondition(values: Seq[(String, Set[String])]): Condition = {
    val ids = dimIdsForAnyFieldsValues(values)
    DimIdNotInExpr(new DimensionExpr(externalLink.dimension), ids)
  }
}
