package org.yupana.externallinks

import org.yupana.api.schema.ExternalLink
import org.yupana.core.TsdbBase

import org.yupana.core.utils.CollectionUtils

abstract class CrossIdBasedExternalLinkService[T <: ExternalLink](tsdb: TsdbBase)
  extends DimIdBasedExternalLinkService[T](tsdb) {

  def tagIdsForCrossJoinedValues(fieldsValues: Map[String, String]): Seq[Long]

  override def dimIdsForAllFieldsValues(fieldsValues: Seq[(String, Set[String])]): Set[Long] = {
    val flatValues = fieldsValues.groupBy(_._1).map { case (k, v) =>
      CollectionUtils.intersectAll(v.map(_._2)).map(k -> _).toList
    }.toList
    val crossed = CollectionUtils.crossJoin(flatValues).map(_.toMap)

    crossed.flatMap { vs =>
      tagIdsForCrossJoinedValues(vs)
    }.toSet
  }

  override def dimIdsForAnyFieldsValues(fieldsValues: Seq[(String, Set[String])]): Set[Long] = {
    fieldsValues.flatMap { case (k, vs) =>
      vs.flatMap(v => tagIdsForCrossJoinedValues(Map(k -> v)))
    }.toSet
  }
}
