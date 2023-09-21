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

package org.yupana.akka

import akka.util.{ ByteString, ByteStringBuilder }

class RepackIterator(it: Iterator[ByteString], packetSize: Int) extends Iterator[ByteString] {

  private val buf = new ByteStringBuilder
  buf.sizeHint(packetSize)

  override def hasNext: Boolean = {
    it.hasNext || buf.nonEmpty
  }

  override def next(): ByteString = {
    while (buf.length < packetSize && it.hasNext) {
      buf.append(it.next())
    }
    val (r, b) = buf.result().splitAt(packetSize)
    buf.clear()
    buf.append(b)
    r
  }
}
