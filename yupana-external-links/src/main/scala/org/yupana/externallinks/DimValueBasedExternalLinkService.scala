package org.yupana.externallinks

import org.yupana.api.query.{Condition, DimensionExpr, In, NotIn}
import org.yupana.api.schema.ExternalLink
import org.yupana.core.utils.{SparseTable, Table}
import org.yupana.core.{Dictionary, TsdbBase}

abstract class DimValueBasedExternalLinkService[T <: ExternalLink](val tsdb: TsdbBase) extends SimpleExternalLinkConditionHandler[T] with SimpleExternalLinkValueExtractor[T] {

  lazy val dictionary: Dictionary = tsdb.dictionary(externalLink.dimension)

  def dimValuesForAllFieldsValues(fieldsValues: Seq[(String, Set[String])]): Set[String]

  def dimValuesForAnyFieldsValues(fieldsValues: Seq[(String, Set[String])]): Set[String]

  def dimIdsForAllFieldsValues(fieldsValues: Seq[(String, Set[String])]): Seq[Long] = {
    dictionary.findIdsByValues(dimValuesForAllFieldsValues(fieldsValues)).values.toSeq
  }

  def dimIdsForAnyFieldsValues(fieldsValues: Seq[(String, Set[String])]): Seq[Long] = {
    dictionary.findIdsByValues(dimValuesForAnyFieldsValues(fieldsValues)).values.toSeq
  }

  override def fieldValuesForDimIds(fields: Set[String], tagIds: Set[Long]): Table[Long, String, String] = {
    val values = dictionary.values(tagIds).map(_.swap)
    if (values.nonEmpty) {
      fieldValuesForDimValues(fields, values.keySet).mapRowKeys(values)
    } else {
      SparseTable.empty
    }
  }

  override def includeCondition(values: Seq[(String, Set[String])]): Condition = {
    val tagValues = dimValuesForAllFieldsValues(values).filter(x => x != null && x.nonEmpty)
    In(DimensionExpr(externalLink.dimension), tagValues)
  }

  override def excludeCondition(values: Seq[(String, Set[String])]): Condition = {
    val tagValues = dimValuesForAnyFieldsValues(values).filter(x => x != null && x.nonEmpty)
    NotIn(DimensionExpr(externalLink.dimension), tagValues)
  }
}
