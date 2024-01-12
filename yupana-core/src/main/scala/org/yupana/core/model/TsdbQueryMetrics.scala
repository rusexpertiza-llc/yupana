/*
 * Copyright 2019 Rusexpertiza LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yupana.core.model

import org.yupana.metrics.QueryStates

import java.time.OffsetDateTime

case class TsdbQueryMetrics(
    queryId: String,
    partitionId: Option[String],
    startDate: OffsetDateTime,
    totalDuration: Long = 0L,
    query: String,
    state: QueryStates.QueryState,
    engine: String,
    metrics: Map[String, MetricData]
)

object TsdbQueryMetrics {
  val initQueryContextQualifier = "init_query_context"
  val createDimensionFiltersQualifier = "create_dimensions_filters"
  val createScansQualifier = "create_scans"
  val scanQualifier = "scan"
  val createContextQualifier = "create_context"
  val loadTagsQualifier = "load_tags"
  val filterRowsQualifier = "filter_rows"
  val filterQualifier = "filter"
  val evaluateExpressionsQualifier = "evaluate_expressions"
  val extractKeyDataQualifier = "extract_key_data"
  val windowFunctionsCheckQualifier = "window_functions_check"
  val windowFunctionsQualifier = "window_functions"
  val mapOperationQualifier = "map_operation"
  val postMapOperationQualifier = "post_map_operation"
  val reduceOperationQualifier = "reduce_operation"
  val postFilterQualifier = "post_filter"
  val collectResultRowsQualifier = "collect_result_rows"
  val dimensionValuesForIdsQualifier = "dimension_values_for_ids"
  val readExternalLinksQualifier: String = "read_external_links"
  val extractDataComputationQualifier = "extract_data_computation"
  val parseScanResultQualifier = "parse_scan_result"
  val dictionaryScanQualifier = "dictionary_scan"

  val queryIdColumn = "query_id"
  val stateColumn = "state"
  val engineColumn = "engine"
  val queryColumn = "query"
  val startDateColumn = "start_date"
  val totalDurationColumn = "total_duration"

  val metricCount = "count"
  val metricTime = "time"
  val metricSpeed = "speed"

  val qualifiers = List(
    initQueryContextQualifier,
    createDimensionFiltersQualifier,
    createScansQualifier,
    createContextQualifier,
    scanQualifier,
    loadTagsQualifier,
    filterRowsQualifier,
    filterQualifier,
    evaluateExpressionsQualifier,
    extractKeyDataQualifier,
    windowFunctionsCheckQualifier,
    windowFunctionsQualifier,
    mapOperationQualifier,
    postMapOperationQualifier,
    reduceOperationQualifier,
    postFilterQualifier,
    collectResultRowsQualifier,
    dimensionValuesForIdsQualifier,
    readExternalLinksQualifier,
    extractDataComputationQualifier,
    parseScanResultQualifier,
    dictionaryScanQualifier
  )
}
