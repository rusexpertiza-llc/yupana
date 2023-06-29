package org.yupana.core.utils.metric

import org.yupana.api.query.Query
import org.yupana.core.model.MetricData
import org.yupana.metrics.QueryStates

case class InternalMetricData(
    query: Query,
    partitionId: Option[String],
    startDate: Long,
    queryState: QueryStates.QueryState,
    totalDuration: Long,
    metricValues: Map[String, MetricData],
    sparkQuery: Boolean
)
