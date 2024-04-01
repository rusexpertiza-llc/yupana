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

package org.yupana.core.utils.metric

import org.yupana.api.query.Query
import org.yupana.metrics.{ Metric, MetricCollector }

trait MetricQueryCollector extends MetricCollector {

  def query: Query
  override def id: String = query.id
  override def meta: String = query.toString
  def isSparkQuery: Boolean
  def user: String

  def initQueryContext: Metric
  def createDimensionFilters: Metric
  def createScans: Metric
  def scan: Metric
  def createContext: Metric
  def readExternalLinks: Metric
  def extractDataComputation: Metric
  def filterRows: Metric
  def filter: Metric
  def evaluateExpressions: Metric
  def extractKeyData: Metric
  def windowFunctions: Metric
  def reduceOperation: Metric
  def postFilter: Metric
  def collectResultRows: Metric
}
