package org.yupana.api.query

import org.yupana.api.schema.MeasureValue

case class DataPoint(
  time: Long,
  tags: Map[String, String],
  value: Seq[MeasureValue]
)
