package org.yupana.core.model

import org.joda.time.LocalDateTime
import org.yupana.core.model.QueryStates.QueryState

case class TsdbQueryMetrics(
                             queryId: String,
                             startDate: LocalDateTime,
                             totalDuration: Double = 0.0,
                             query: String,
                             state: QueryState,
                             metrics: Map[String, MetricData]
                           )

object TsdbQueryMetrics {
  val createQueriesQualifier = "create_queries"
  val createDimensionFiltersQualifier = "create_dimensions_filters"
  val createScansQualifier = "create_scans"
  val loadTagsQualifier = "load_tags"
  val filterRowsQualifier = "filter_rows"
  val windowFunctionsCheckQualifier = "window_functions_check"
  val windowFunctionsQualifier = "window_functions"
  val mapOperationQualifier = "map_operation"
  val postMapOperationQualifier = "post_map_operation"
  val reduceOperationQualifier = "reduce_operation"
  val postFilterQualifier = "post_filter"
  val collectResultRowsQualifier = "collect_result_rows"
  val extractDataTagsQualifier = "extract_data_tags"
  val extractDataComputationQualifier = "extract_data_computation"
  val getResultQualifier = "get_result"
  val parseResultQualifier = "parse_result"

  val queryIdColumn = "query_id"
  val stateColumn = "state"
  val queryColumn = "query"
  val startDateColumn = "start_date"
  val totalDurationColumn = "total_duration"

  val metricCount = "count"
  val metricTime = "time"
  val metricSpeed = "speed"

  val qualifiers = List(
    createQueriesQualifier, createDimensionFiltersQualifier, createScansQualifier, loadTagsQualifier,
    filterRowsQualifier, windowFunctionsCheckQualifier, windowFunctionsQualifier, mapOperationQualifier,
    postMapOperationQualifier, reduceOperationQualifier, postFilterQualifier, collectResultRowsQualifier,
    extractDataTagsQualifier, extractDataComputationQualifier, getResultQualifier, parseResultQualifier
  )
}

case class MetricData(count: Long, time: Double, speed: Double)

object QueryStates {

  sealed abstract class QueryState(val name: String) {
    override def toString: String = name
  }

  case object Running extends QueryState("RUNNING")

  case object Finished extends QueryState("FINISHED")

  case object Cancelled extends QueryState("CANCELLED")

  val states = List(Running, Finished, Cancelled)

  def getByName(name: String): QueryState = states.find(_.name == name).getOrElse(throw new IllegalArgumentException(s"State with name $name not found"))

}