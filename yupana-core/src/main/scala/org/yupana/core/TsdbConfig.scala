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

package org.yupana.core

trait TsdbConfig {
  val collectMetrics: Boolean
  val metricsUpdateInterval: Int
  val extractBatchSize: Int
  val putBatchSize: Int
  val putEnabled: Boolean
  val maxRegions: Int
  val reduceLimit: Int
}

case class SimpleTsdbConfig(
    collectMetrics: Boolean = false,
    metricsUpdateInterval: Int = 30000,
    extractBatchSize: Int = 10000,
    putBatchSize: Int = 1000,
    putEnabled: Boolean = false,
    maxRegions: Int = 50,
    reduceLimit: Int = Int.MaxValue
) extends TsdbConfig
