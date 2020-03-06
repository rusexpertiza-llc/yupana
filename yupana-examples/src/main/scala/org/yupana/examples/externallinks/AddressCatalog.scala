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

import org.yupana.api.schema.{ Dimension, ExternalLink, LinkMetric }
import org.yupana.core.TsdbBase
import org.yupana.core.utils.{ CollectionUtils, SparseTable, Table }
import org.yupana.externallinks.DimValueBasedExternalLinkService
import org.yupana.schema.Dimensions

trait AddressCatalog extends ExternalLink {
  val CITY = "city"

  override val linkName: String = "AddressCatalog"
  override val dimension: Dimension = Dimensions.KKM_ID_TAG
  override val fieldsNames: Set[LinkMetric] = Set(LinkMetric[String](CITY))
}

object AddressCatalog extends AddressCatalog

class AddressCatalogImpl(tsdb: TsdbBase, override val externalLink: AddressCatalog)
    extends DimValueBasedExternalLinkService[AddressCatalog](tsdb) {

  val kkmAddressData: Seq[(String, String)] = (1 to 20).map(id => (id.toString, if (id < 15) "Москва" else "Таганрог"))

  override def dimValuesForAllFieldsValues(fieldsValues: Seq[(String, Set[Any])]): Set[String] = {
    val ids = fieldsValues.map {
      case (AddressCatalog.CITY, cities) => kkmAddressData.filter(x => cities.contains(x._2)).map(_._1).toSet
      case (f, _)                        => throw new IllegalArgumentException(s"Unknown field $f")
    }

    CollectionUtils.intersectAll(ids)
  }

  override def dimValuesForAnyFieldsValues(fieldsValues: Seq[(String, Set[Any])]): Set[String] = {
    val ids = fieldsValues.map {
      case (AddressCatalog.CITY, cities) => kkmAddressData.filter(x => cities.contains(x._2)).map(_._1).toSet
      case (f, _)                        => throw new IllegalArgumentException(s"Unknown field $f")
    }

    ids.fold(Set.empty)(_ union _)
  }

  override def fieldValuesForDimValues(fields: Set[String], kkmIds: Set[String]): Table[String, String, Any] = {
    val unknownFields = fields.filterNot(_ == AddressCatalog.CITY)
    if (unknownFields.nonEmpty) throw new IllegalArgumentException(s"Unknown fields $unknownFields")

    val values = kkmAddressData
      .filter(x => kkmIds.contains(x._1))
      .map { case (kkmId, city) => kkmId -> Map(AddressCatalog.CITY -> city) }
      .toMap
    SparseTable(values)
  }
}
