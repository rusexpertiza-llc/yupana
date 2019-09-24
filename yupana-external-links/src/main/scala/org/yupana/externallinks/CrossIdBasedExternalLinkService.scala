package org.yupana.externallinks

import org.yupana.api.schema.ExternalLink
import org.yupana.api.utils.SortedSetIterator
import org.yupana.core.TsdbBase
import org.yupana.core.utils.CollectionUtils

abstract class CrossIdBasedExternalLinkService[T <: ExternalLink](tsdb: TsdbBase)
  extends DimIdBasedExternalLinkService[T](tsdb) {

  def dimIdsForCrossJoinedValues(fieldsValues: Map[String, String]): Seq[Long]

  override def dimIdsForAllFieldsValues(fieldsValues: Seq[(String, Set[String])]): SortedSetIterator[Long] = {
    val flatValues = fieldsValues.groupBy(_._1).map { case (k, v) =>
      CollectionUtils.intersectAll(v.map(_._2)).map(k -> _).toList
    }.toList
    val crossed = CollectionUtils.crossJoin(flatValues).map(_.toMap)
    val dimIds = crossed.flatMap { vs =>
      dimIdsForCrossJoinedValues(vs)
    }
    SortedSetIterator(dimIds.sorted.iterator)
  }

  override def dimIdsForAnyFieldsValues(fieldsValues: Seq[(String, Set[String])]): SortedSetIterator[Long] = {
    val dimIds = fieldsValues.flatMap { case (k, vs) =>
      vs.flatMap(v => dimIdsForCrossJoinedValues(Map(k -> v)))
    }
    SortedSetIterator(dimIds.sorted.iterator)
  }
}
