package org.yupana.api.query

import org.yupana.api.schema.MetricValue

case class DataPoint(
  time: Long,
  dimensions: Map[String, String],
  metrics: Seq[MetricValue]
)
