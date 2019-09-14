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
