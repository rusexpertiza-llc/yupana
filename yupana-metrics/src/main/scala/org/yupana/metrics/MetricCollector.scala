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

package org.yupana.metrics

import java.util.concurrent.atomic.AtomicReference

trait MetricCollector extends Serializable {
  def id: String
  def operationName: String
  def partitionId: Option[String]

  lazy val fullId: String = partitionId.map(pId => s"${id}_$pId").getOrElse(id)

  def meta: String = ""

  protected var startAt: Long = 0L
  protected var finishAt: Long = 0L
  private val qs: AtomicReference[QueryStatus] = new AtomicReference[QueryStatus](Unknown)

  def dynamicMetric(name: String): Metric

  def isEnabled: Boolean

  def start(): Unit = startAt = System.nanoTime()
  def checkpoint(): Unit
  def metricUpdated(metric: Metric, time: Long): Unit
  def finish(): Unit = finishAt = System.nanoTime()

  def allMetrics: Seq[Metric]

  def startTime: Long = System.currentTimeMillis()
  def resultDuration: Long = finishAt - startAt

  def queryStatus: QueryStatus = qs.get()

  def setQueryStatus(newStatus: QueryStatus): Unit = qs.lazySet(newStatus)
}

object MetricCollector {
  def asSeconds(nanoTime: Long): Double = nanoTime / 1000000000.0
  def asMillis(nanoTime: Long): Double = nanoTime / 1000000.0
}
