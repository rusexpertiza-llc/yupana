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

package org.yupana.jdbc

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ Future, Promise }

class Countdown(n: Int) {
  private val value = new AtomicInteger(n)
  private val p = Promise[Int]()

  def release(): Int = {
    val i = value.decrementAndGet()
    if (i == 0) p.success(i)
    i
  }

  def cancel(): Unit = {
    p.success(value.get())
  }

  def failure(t: Throwable): Unit = {
    p.failure(t)
  }

  def future: Future[Int] = p.future
}
