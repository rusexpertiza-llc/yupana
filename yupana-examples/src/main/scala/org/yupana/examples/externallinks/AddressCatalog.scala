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

import org.yupana.api.Time
import org.yupana.api.query._
import org.yupana.api.schema.{ Dimension, ExternalLink, LinkField, Schema }
import org.yupana.core.ExternalLinkService
import org.yupana.core.auth.YupanaUser
import org.yupana.core.model.BatchDataset
import org.yupana.core.utils.{ CollectionUtils, FlatAndCondition, SparseTable, Table }
import org.yupana.externallinks.ExternalLinkUtils
import org.yupana.schema.Dimensions

trait AddressCatalog extends ExternalLink {
  val CITY = "city"
  val LAT = "lat"
  val LON = "lon"

  override type DimType = Int

  override val linkName: String = "AddressCatalog"
  override val dimension: Dimension.Aux[Int] = Dimensions.KKM_ID
  override val fields: Set[LinkField] = Set(LinkField[String](CITY), LinkField[Double](LAT), LinkField[Double](LON))
}

object AddressCatalog extends AddressCatalog

case class AddressData(city: String, lat: Double, lon: Double) {
  def asMap: Map[String, Any] = Map(
    AddressCatalog.CITY -> city,
    AddressCatalog.LAT -> lat,
    AddressCatalog.LON -> lon
  )
}

class AddressCatalogImpl(override val schema: Schema, override val externalLink: AddressCatalog)
    extends ExternalLinkService[AddressCatalog] {
  import syntax.All._

  val kkmAddressData: Seq[(Int, AddressData)] =
    (1 to 20).map { id =>
      (
        id,
        if (id < 15) AddressData("Москва", -26.668287, 153.102198) else AddressData("Таганрог", 61.314494, 10.303508)
      )
    }

  override def put(dataPoints: Seq[DataPoint]): Unit = {}

  override def put(batchDataset: BatchDataset): Unit = {}

  override def setLinkedValues(
      batch: BatchDataset,
      exprs: Set[LinkExpr[_]]
  ): Unit = {
    ExternalLinkUtils.setLinkedValues(
      externalLink,
      batch,
      exprs,
      fieldValuesForDimValues
    )
  }

  override def transformCondition(
      condition: FlatAndCondition,
      startTime: Time,
      user: YupanaUser
  ): Seq[ConditionTransformation] = {
    ExternalLinkUtils.transformCondition(
      externalLink.linkName,
      condition,
      includeTransform,
      excludeTransform
    )
  }

  private def idsForValues(values: Seq[(SimpleCondition, String, Set[Any])]): Seq[Set[Int]] = {
    values.map {
      case (_, AddressCatalog.CITY, cities)   => kkmAddressData.filter(x => cities.contains(x._2.city)).map(_._1).toSet
      case (_, AddressCatalog.LAT, latitudes) =>
        kkmAddressData.collect { case (value, coord) if latitudes.contains(coord.lat) => value }.toSet
      case (_, AddressCatalog.LON, longitudes) =>
        kkmAddressData.collect { case (value, coord) if longitudes.contains(coord.lon) => value }.toSet
      case (_, f, _) => throw new IllegalArgumentException(s"Unknown field $f")
    }
  }

  private def includeTransform(values: Seq[(SimpleCondition, String, Set[Any])]): Seq[ConditionTransformation] = {
    val ids = idsForValues(values)
    val dimValues = CollectionUtils.intersectAll(ids)
    ConditionTransformation.replace(
      values.map(_._1),
      in(dimension(externalLink.dimension.aux), dimValues)
    )
  }

  private def excludeTransform(values: Seq[(SimpleCondition, String, Set[Any])]): Seq[ConditionTransformation] = {
    val ids = idsForValues(values)
    val dimValues = ids.fold(Set.empty)(_ union _)
    ConditionTransformation.replace(
      values.map(_._1),
      notIn(dimension(externalLink.dimension.aux), dimValues)
    )
  }

  private def fieldValuesForDimValues(fields: Set[String], kkmIds: Set[Int]): Table[Int, String, Any] = {
    val unknownFields = fields.diff(externalLink.fields.map(_.name))
    if (unknownFields.nonEmpty) throw new IllegalArgumentException(s"Unknown fields $unknownFields")

    val values = kkmAddressData
      .filter(x => kkmIds.contains(x._1))
      .map { case (kkmId, addr) => kkmId -> addr.asMap.filter { case (k, _) => fields.contains(k) } }
      .toMap
    SparseTable(values)
  }
}
