package org.yupana.api.query

import org.yupana.api.schema.MeasureValue

case class DataPoint(
  time: Long,
  dimensions: Map[String, String],
  measures: Seq[MeasureValue]
)
