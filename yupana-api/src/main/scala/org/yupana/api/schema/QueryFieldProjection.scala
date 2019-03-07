package org.yupana.api.schema

import org.yupana.api.query.QueryField

sealed trait QueryFieldProjection {
  val queryField: QueryField
}

case class QueryFieldToTime(override val queryField: QueryField) extends QueryFieldProjection
case class QueryFieldToDimension(override val queryField: QueryField, dimensionName: String) extends QueryFieldProjection
case class QueryFieldToMetric(override val queryField: QueryField, metric: Metric) extends QueryFieldProjection
