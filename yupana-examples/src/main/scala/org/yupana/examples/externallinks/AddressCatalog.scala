package org.yupana.examples.externallinks

import org.yupana.api.schema.{Dimension, ExternalLink}
import org.yupana.core.TsdbBase
import org.yupana.core.utils.{CollectionUtils, SparseTable, Table}
import org.yupana.externallinks.DimValueBasedExternalLinkService
import org.yupana.schema.Dimensions

trait AddressCatalog extends ExternalLink {
  val CITY = "city"

  override val linkName: String = "AddressCatalog"
  override val dimension: Dimension = Dimensions.KKM_ID_TAG
  override val fieldsNames: Set[String] = Set(CITY)
}

object AddressCatalog extends AddressCatalog

class AddressCatalogImpl(tsdb: TsdbBase, override val externalLink: AddressCatalog)
  extends DimValueBasedExternalLinkService[AddressCatalog](tsdb) {

  val kkmAddressData: Seq[(String, String)] = (1 to 20).map(id => (id.toString, if (id < 15) "Москва" else "Таганрог"))

  override def dimValuesForAllFieldsValues(fieldsValues: Seq[(String, Set[String])]): Set[String] = {
    val ids = fieldsValues.map {
      case (AddressCatalog.CITY, cities) => kkmAddressData.filter(x => cities.contains(x._2)).map(_._1).toSet
      case (f, _) => throw new IllegalArgumentException(s"Unknown field $f")
    }

    CollectionUtils.intersectAll(ids)
  }

  override def dimValuesForAnyFieldsValues(fieldsValues: Seq[(String, Set[String])]): Set[String] = {
    val ids = fieldsValues.map {
      case (AddressCatalog.CITY, cities) => kkmAddressData.filter(x => cities.contains(x._2)).map(_._1).toSet
      case (f, _) => throw new IllegalArgumentException(s"Unknown field $f")
    }

    ids.fold(Set.empty)(_ union _)
  }

  override def fieldValuesForDimValues(fields: Set[String], kkmIds: Set[String]): Table[String, String, String] = {
    val unknownFields = fields.filterNot(_ == AddressCatalog.CITY)
    if (unknownFields.nonEmpty) throw new IllegalArgumentException(s"Unknown fields $unknownFields")

    val values = kkmAddressData.filter(x => kkmIds.contains(x._1))
      .map { case (kkmId, city) => kkmId -> Map(AddressCatalog.CITY -> city) }
      .toMap
    SparseTable(values)
  }
}
