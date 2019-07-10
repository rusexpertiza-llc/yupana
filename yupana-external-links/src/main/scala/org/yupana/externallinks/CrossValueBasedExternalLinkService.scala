package org.yupana.externallinks

import org.yupana.api.schema.ExternalLink
import org.yupana.core.TsdbBase
import org.yupana.core.utils.CollectionUtils

abstract class CrossValueBasedExternalLinkService[T <: ExternalLink](tsdb: TsdbBase) extends DimValueBasedExternalLinkService[T](tsdb) {

  def tagValuesForCrossJoinedValues(fieldsValues: Map[String, String]): Seq[String]

  override def dimValuesForAllFieldsValues(fieldsValues: Seq[(String, Set[String])]): Set[String] = {
    val flatValues = fieldsValues.groupBy(_._1).map { case (k, vs) =>
      CollectionUtils.intersectAll(vs.map(_._2)).toList.map(k -> _)
    }.toList

    val crossed = CollectionUtils.crossJoin(flatValues).map(_.toMap)

    crossed.flatMap { vs =>
      tagValuesForCrossJoinedValues(vs)
    }.toSet
  }

  override def dimValuesForAnyFieldsValues(fieldsValues: Seq[(String, Set[String])]): Set[String] = {
    fieldsValues.flatMap { case (k, vs) =>
      vs.flatMap(v => tagValuesForCrossJoinedValues(Map(k -> v)))
    }.toSet
  }
}
