package org.yupana.core.dao

import org.yupana.api.query.Query
import org.yupana.core.model.QueryStates.QueryState
import org.yupana.core.model.{MetricData, TsdbQueryMetrics}

trait TsdbQueryMetricsDao {
  def initializeQueryMetrics(query: Query, sparkQuery: Boolean): Unit
  def queriesByFilter(filter: QueryMetricsFilter): List[TsdbQueryMetrics]
  def updateQueryMetrics(queryId: String,
                         queryState: QueryState,
                         totalDuration: Double,
                         metricValues: Map[String, MetricData],
                         sparkQuery: Boolean): Unit
  def setQueryState(queryId: String, queryState: QueryState): Boolean
}

case class QueryMetricsFilter(queryId: Option[String], limit: Int = 1)