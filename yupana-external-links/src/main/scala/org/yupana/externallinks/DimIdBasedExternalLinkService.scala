package org.yupana.externallinks

import org.yupana.api.query._
import org.yupana.api.schema.ExternalLink
import org.yupana.api.utils.SortedSetIterator
import org.yupana.core.{Dictionary, TsdbBase}
import org.yupana.core.utils.{SparseTable, Table}

abstract class DimIdBasedExternalLinkService[T <: ExternalLink](val tsdb: TsdbBase) extends SimpleExternalLinkConditionHandler[T] with SimpleExternalLinkValueExtractor[T] {

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
    DimIdIn(new DimensionExpr(externalLink.dimension), ids)
  }

  override def excludeCondition(values: Seq[(String, Set[String])]): Condition = {
    val ids = dimIdsForAnyFieldsValues(values)
    DimIdNotIn(new DimensionExpr(externalLink.dimension), ids)
  }
}
