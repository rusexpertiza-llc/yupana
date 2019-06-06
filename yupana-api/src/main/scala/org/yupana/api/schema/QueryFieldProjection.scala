package org.yupana.api.schema

import org.yupana.api.query.QueryField

/**
  * Defines field projection in rollup.
  */
sealed trait QueryFieldProjection {
  /** Query field to be projected */
  val queryField: QueryField
}

/**
  * Projects query field to time.
  */
case class QueryFieldToTime(override val queryField: QueryField) extends QueryFieldProjection

/**
  * Projects query field to dimension in output table
  * @param queryField query field to be projected
  * @param dimension dimension to store value
  */
case class QueryFieldToDimension(override val queryField: QueryField, dimension: Dimension) extends QueryFieldProjection

/**
  * Projects query field to metric in output table
  * @param queryField query field to be projected
  * @param metric metric in output table to store value
  */
case class QueryFieldToMetric(override val queryField: QueryField, metric: Metric) extends QueryFieldProjection
