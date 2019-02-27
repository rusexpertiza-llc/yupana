package org.yupana.api.schema

import org.joda.time.DateTimeFieldType
import org.yupana.api.query._
import org.yupana.api.types.UnaryOperation

class Rollup(
  val name: String,
  val filter: Option[Condition],
  groupBy: Seq[Expression],
  fields: Seq[QueryFieldProjection],
  val downsamplingInterval: Option[DateTimeFieldType],
  val fromTable: Table,
  val toTable: Table
) extends Serializable {

  lazy val timeExpr = downsamplingInterval match {
    case Some(d) =>
      FunctionExpr(UnaryOperation.trunc(d), TimeExpr)
    case None =>
      TimeExpr
  }

  lazy val timeField = timeExpr as Table.TIME_FIELD_NAME
  lazy val allFields = QueryFieldToTime(timeField) +: fields
  lazy val allGroupBy = if (downsamplingInterval.isDefined) timeExpr +: groupBy else groupBy

  lazy val tagResultNameMap: Map[String, String] = allFields.collect {
    case QueryFieldToTag(queryField, tagName) =>
      tagName -> queryField.name
  }.toMap

  lazy val fieldNamesMap: Map[String, String] = allFields.collect {
    case QueryFieldToValue(queryField, field) =>
      field.name -> queryField.name
  }.toMap

  def getResultFieldForTagName(tagName:String): String = {
    tagResultNameMap.getOrElse(tagName, throw new Exception(s"Can't find result field for tag name: $tagName"))
  }

  def getResultFieldForFieldName(fieldName:String): String = {
    fieldNamesMap.getOrElse(fieldName, throw new Exception(s"Can't find result field for field name: $fieldName"))
  }
}

sealed trait QueryFieldProjection {
  val queryField: QueryField
}

case class QueryFieldToTime(override val queryField: QueryField) extends QueryFieldProjection
case class QueryFieldToTag(override val queryField: QueryField, tagName: String) extends QueryFieldProjection
case class QueryFieldToValue(override val queryField: QueryField, field: Measure) extends QueryFieldProjection
