package org.yupana.externallinks

import org.yupana.api.query.{DimensionExpr, Expression, LinkExpr}
import org.yupana.api.schema.ExternalLink
import org.yupana.core.ExternalLinkService
import org.yupana.core.model.InternalRow
import org.yupana.core.utils.Table

trait SimpleExternalLinkValueExtractor[T <: ExternalLink] extends ExternalLinkService[T] {

  def fieldValuesForDimValues(fields: Set[String], tagValues: Set[String]): Table[String, String, String]

  def fieldValuesForDimIds(fields: Set[String], tagIds: Set[Long]): Table[Long, String, String]

  def fieldValueForDimValue(fieldName: String, tagValue: String): Option[String] = {
    fieldValuesForDimValues(Set(fieldName), Set(tagValue)).get(tagValue, fieldName)
  }

  def fieldValueForDimId(fieldName: String, tagId: Long): Option[String] ={
    fieldValuesForDimIds(Set(fieldName), Set(tagId)).get(tagId, fieldName)
  }

  override def setLinkedValues(exprIndex: scala.collection.Map[Expression, Int], valueData: Seq[InternalRow], exprs: Set[LinkExpr]): Unit = {
    val dimExpr = DimensionExpr(externalLink.dimension)
    val fields = exprs.map(_.linkField)
    val dimValues = valueData.flatMap(_.get[String](exprIndex, dimExpr)).toSet

    val allFieldValues = fieldValuesForDimValues(fields, dimValues)

    valueData.foreach { vd =>
      vd.get[String](exprIndex, dimExpr).foreach { dimValue =>
        allFieldValues.row(dimValue).foreach { case (field, value) =>
          val linkExpr = LinkExpr(externalLink, field)
          if (value != null && exprIndex.contains(linkExpr)) vd.set(exprIndex, linkExpr, Some(value))
        }
      }
    }
  }
}
