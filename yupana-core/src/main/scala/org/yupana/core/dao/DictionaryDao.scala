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

package org.yupana.core.dao

import org.yupana.api.schema.Dimension
import org.yupana.core.utils.metric.MetricQueryCollector

trait DictionaryDao {
  def createSeqId(dimension: Dimension): Int
  def getValueById(dimension: Dimension, id: Long): Option[String]
  def getValuesByIds(dimension: Dimension, ids: Set[Long], metricCollector: MetricQueryCollector): Map[Long, String]
  def getIdByValue(dimension: Dimension, value: String): Option[Long]
  def getIdsByValues(dimension: Dimension, value: Set[String]): Map[String, Long]
  def checkAndPut(dimension: Dimension, id: Long, value: String): Boolean
}
