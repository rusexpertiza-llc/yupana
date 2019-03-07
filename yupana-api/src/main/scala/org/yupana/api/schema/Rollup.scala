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

  lazy val timeExpr: Expression = downsamplingInterval match {
    case Some(d) =>
      FunctionExpr(UnaryOperation.trunc(d), TimeExpr)
    case None =>
      TimeExpr
  }

  lazy val timeField: QueryField = timeExpr as Table.TIME_FIELD_NAME
  lazy val allFields: Seq[QueryFieldProjection] = QueryFieldToTime(timeField) +: fields
  lazy val allGroupBy: Seq[Expression] = if (downsamplingInterval.isDefined) timeExpr +: groupBy else groupBy

  lazy val tagResultNameMap: Map[String, String] = allFields.collect {
    case QueryFieldToDimension(queryField, tagName) =>
      tagName -> queryField.name
  }.toMap

  lazy val fieldNamesMap: Map[String, String] = allFields.collect {
    case QueryFieldToMetric(queryField, field) =>
      field.name -> queryField.name
  }.toMap

  def getResultFieldForDimName(dimName: String): String = {
    tagResultNameMap.getOrElse(dimName, throw new Exception(s"Can't find result field for tag name: $dimName"))
  }

  def getResultFieldForMeasureName(fieldName: String): String = {
    fieldNamesMap.getOrElse(fieldName, throw new Exception(s"Can't find result field for field name: $fieldName"))
  }
}
