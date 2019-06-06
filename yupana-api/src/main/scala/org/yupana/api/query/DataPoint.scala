package org.yupana.api.query

import org.yupana.api.schema.{Dimension, MetricValue, Table}

case class DataPoint(
  table: Table,
  time: Long,
  dimensions: Map[Dimension, String],
  metrics: Seq[MetricValue]
)
