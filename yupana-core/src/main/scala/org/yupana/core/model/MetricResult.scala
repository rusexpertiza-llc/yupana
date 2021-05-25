package org.yupana.core.model

case class MetricResult(
    queryId: String,
    state: String,
    isSparkContext: Boolean = false,
    metricsData: Map[String, MetricData] = Map(),
    durationSec: Double
)
