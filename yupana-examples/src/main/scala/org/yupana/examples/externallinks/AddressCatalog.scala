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

package org.yupana.examples.externallinks

import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.schema.{ Dimension, ExternalLink }
import org.yupana.core.ExternalLinkService
import org.yupana.core.model.InternalRow
import org.yupana.core.utils.{ CollectionUtils, SparseTable, Table }
import org.yupana.externallinks.ExternalLinkUtils
import org.yupana.schema.Dimensions

trait AddressCatalog extends ExternalLink {
  val CITY = "city"

  override val linkName: String = "AddressCatalog"
  override val dimension: Dimension.Aux[Int] = Dimensions.KKM_ID
  override val fieldsNames: Set[String] = Set(CITY)
}

object AddressCatalog extends AddressCatalog

class AddressCatalogImpl(override val externalLink: AddressCatalog) extends ExternalLinkService[AddressCatalog] {
  import syntax.All._

  val kkmAddressData: Seq[(Int, String)] = (1 to 20).map(id => (id, if (id < 15) "Москва" else "Таганрог"))

  override def setLinkedValues(
      exprIndex: collection.Map[Expression, Int],
      rows: Seq[InternalRow],
      exprs: Set[LinkExpr]
  ): Unit = {
    ExternalLinkUtils.setLinkedValues(
      externalLink,
      exprIndex,
      rows,
      exprs,
      fieldValuesForDimValues
    )
  }

  override def condition(condition: Condition): Condition = {
    ExternalLinkUtils.transformCondition(externalLink.linkName, condition, createInclude, createExclude)
  }

  private def createInclude(values: Seq[(String, Set[String])]): Condition = {

    val ids = values.map {
      case (AddressCatalog.CITY, cities) => kkmAddressData.filter(x => cities.contains(x._2)).map(_._1).toSet
      case (f, _)                        => throw new IllegalArgumentException(s"Unknown field $f")
    }

    val dimValues = CollectionUtils.intersectAll(ids)

    in(dimension(externalLink.dimension.aux), dimValues)
  }

  private def createExclude(values: Seq[(String, Set[String])]): Condition = {
    val ids = values.map {
      case (AddressCatalog.CITY, cities) => kkmAddressData.filter(x => cities.contains(x._2)).map(_._1).toSet
      case (f, _)                        => throw new IllegalArgumentException(s"Unknown field $f")
    }

    val dimValues = ids.fold(Set.empty)(_ union _)
    notIn(dimension(externalLink.dimension.aux), dimValues)
  }

  private def fieldValuesForDimValues(fields: Set[String], kkmIds: Set[Int]): Table[Int, String, String] = {
    val unknownFields = fields.filterNot(_ == AddressCatalog.CITY)
    if (unknownFields.nonEmpty) throw new IllegalArgumentException(s"Unknown fields $unknownFields")

    val values = kkmAddressData
      .filter(x => kkmIds.contains(x._1))
      .map { case (kkmId, city) => kkmId -> Map(AddressCatalog.CITY -> city) }
      .toMap
    SparseTable(values)
  }
}
