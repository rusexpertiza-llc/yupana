package org.yupana.examples.externallinks

import org.yupana.api.schema.{ Dimension, ExternalLink, LinkMetric }
import org.yupana.core.TsdbBase
import org.yupana.core.utils.{ SparseTable, Table }
import org.yupana.externallinks.DimValueBasedExternalLinkService
import org.yupana.schema.Dimensions

trait GeoCatalog extends ExternalLink {

  val LAT = "latitude"
  val LON = "longitude"

  override val linkName: String = "GeoCatalog"
  override val dimension: Dimension = Dimensions.KKM_ID_TAG
  override val fieldsNames: Set[LinkMetric] = Set(LAT, LON) map LinkMetric[Double]
}

object GeoCatalog extends GeoCatalog

class GeoCatalogImpl(tsdb: TsdbBase, override val externalLink: GeoCatalog)
    extends DimValueBasedExternalLinkService[GeoCatalog](tsdb) {

  val kkmGeoData: Seq[(Double, Double, String)] =
    (1 to 20).map(id => if (id < 15) (56.1, 47.2, id.toString) else (56.09, 47.21, id.toString))

  override def dimValuesForAllFieldsValues(fieldsValues: Seq[(String, Set[Any])]): Set[String] = {
    fieldsValues
      .foldLeft(kkmGeoData)({
        case (acc, (GeoCatalog.LAT, latitudes))  => acc.filter { case (lat, _, _) => latitudes.contains(lat) }
        case (acc, (GeoCatalog.LON, longitudes)) => acc.filter { case (_, lon, _) => longitudes.contains(lon) }
        case (_, (f, _))                         => throw new IllegalArgumentException(s"Unknown field $f")
      })
      .map(_._3)
      .toSet
  }

  override def dimValuesForAnyFieldsValues(fieldsValues: Seq[(String, Set[Any])]): Set[String] = {
    val ids = fieldsValues.map {
      case (GeoCatalog.LAT, latitudes)  => kkmGeoData.filter(x => latitudes.contains(x._1)).map(_._3).toSet
      case (GeoCatalog.LON, longitudes) => kkmGeoData.filter(x => longitudes.contains(x._2)).map(_._3).toSet
      case (f, _)                       => throw new IllegalArgumentException(s"Unknown field $f")
    }
    ids.fold(Set.empty)(_ union _)
  }

  override def fieldValuesForDimValues(fields: Set[String], kkmIds: Set[String]): Table[String, String, Any] = {
    val unknownFields = fields.diff(GeoCatalog.fieldsNames.map(_.name))
    if (unknownFields.nonEmpty) throw new IllegalArgumentException(s"Unknown fields $unknownFields")

    val values = kkmGeoData
      .filter(x => kkmIds.contains(x._3))
      .map { case (lat, lon, kkmId) => kkmId -> Map(GeoCatalog.LAT -> lat, GeoCatalog.LON -> lon) }
      .toMap
    SparseTable(values)
  }
}
