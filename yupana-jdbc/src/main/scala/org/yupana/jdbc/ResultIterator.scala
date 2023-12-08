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

import org.yupana.protocol.ResultRow

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class ResultIterator(id: Int, tcpClient: YupanaTcpClient)(implicit ec: ExecutionContext) extends Iterator[ResultRow] {

  private val buffer: mutable.Queue[ResultRow] = mutable.Queue.empty
  private var done = false

  def addResult(result: ResultRow): Unit = buffer.enqueue(result)
  def setDone(): Unit = done = true

  override def hasNext: Boolean = {
    if (!done && buffer.isEmpty) {
      tcpClient.acquireNext(id)
    }

    buffer.nonEmpty
  }

  override def next(): ResultRow = {
    if (!hasNext) throw new NoSuchElementException("Next on empty iterator")
    buffer.dequeue()
  }
}
