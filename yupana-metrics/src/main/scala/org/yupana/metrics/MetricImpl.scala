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

import java.util.concurrent.atomic.AtomicLong

class MetricImpl(
    val name: String,
    metricCollector: MetricCollector
) extends Metric {

  private val countAdder = new AtomicLong()
  private val timeAdder = new AtomicLong()

  override def time: Long = timeAdder.get()
  override def count: Long = countAdder.get()

  override def reset(): Unit = {
    countAdder.set(0)
    timeAdder.set(0)
  }

  override def measure[T](cnt: Int)(f: => T): T =
    try {
      val start = System.nanoTime()
      val result = f
      countAdder.addAndGet(cnt)
      val end = System.nanoTime()
      timeAdder.addAndGet(end - start)
      metricCollector.metricUpdated(this, end)
      result
    } catch {
      case throwable: Throwable =>
        metricCollector.setQueryStatus(Failed(throwable))
        metricCollector.finish()
        throw throwable
    }
}
