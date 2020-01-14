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

import org.yupana.api.query.{ DimensionExpr, Expression, LinkExpr }
import org.yupana.api.schema.ExternalLink
import org.yupana.core.ExternalLinkService
import org.yupana.core.model.InternalRow
import org.yupana.core.utils.Table
import org.yupana.core.utils.metric.MetricQueryCollector

trait SimpleExternalLinkValueExtractor[T <: ExternalLink] extends ExternalLinkService[T] {

  def fieldValuesForDimValues(
      fields: Set[String],
      tagValues: Set[String],
      metricCollector: MetricQueryCollector
  ): Table[String, String, String]

  def fieldValuesForDimIds(
      fields: Set[String],
      tagIds: Set[Long],
      metricCollector: MetricQueryCollector
  ): Table[Long, String, String]

  def fieldValueForDimValue(
      fieldName: String,
      tagValue: String,
      metricCollector: MetricQueryCollector
  ): Option[String] = {
    fieldValuesForDimValues(Set(fieldName), Set(tagValue), metricCollector).get(tagValue, fieldName)
  }

  def fieldValueForDimId(fieldName: String, tagId: Long, metricCollector: MetricQueryCollector): Option[String] = {
    fieldValuesForDimIds(Set(fieldName), Set(tagId), metricCollector).get(tagId, fieldName)
  }

  override def setLinkedValues(
      exprIndex: scala.collection.Map[Expression, Int],
      valueData: Seq[InternalRow],
      exprs: Set[LinkExpr],
      metricCollector: MetricQueryCollector
  ): Unit = {
    val dimExpr = DimensionExpr(externalLink.dimension)
    val fields = exprs.map(_.linkField)
    val dimValues = valueData.flatMap(_.get[String](exprIndex, dimExpr)).toSet

    val allFieldValues = fieldValuesForDimValues(fields, dimValues, metricCollector)

    valueData.foreach { vd =>
      vd.get[String](exprIndex, dimExpr).foreach { dimValue =>
        allFieldValues.row(dimValue).foreach {
          case (field, value) =>
            val linkExpr = LinkExpr(externalLink, field)
            if (value != null && exprIndex.contains(linkExpr)) vd.set(exprIndex, linkExpr, Some(value))
        }
      }
    }
  }
}
