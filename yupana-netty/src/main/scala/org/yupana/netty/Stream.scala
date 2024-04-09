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

package org.yupana.netty
import io.netty.buffer.{ ByteBuf, Unpooled }
import org.yupana.api.query.Result
import org.yupana.api.types.{ ID, ReaderWriter }
import org.yupana.protocol.{ Response, ResultFooter, ResultRow }

class Stream(id: Int, result: Result) {

  private var rows = 0
  private val resultTypes = result.dataTypes.zipWithIndex
  val MAX_VALUE_SIZE = 2_000_000
  private val buffer = Unpooled.buffer(MAX_VALUE_SIZE)
  implicit val readerWriter: ReaderWriter[ByteBuf, ID, Int, Int] = ByteBufEvalReaderWriter

  def close(): Unit = result.close()

  def next(count: Int): Seq[Response[_]] = {

    val batch = createBatch(result, count)
    if (result.isLast()) {
      batch :+ createFooter(result)
    } else {
      batch
    }
  }

  def hasNext: Boolean = !result.isLast()

  private def createBatch(result: Result, size: Int): Seq[ResultRow] = {
    var seq = Seq.empty[ResultRow]

    var i = 0
    while (i < size && result.next()) {
      val bytes = resultTypes.map {
        case (rt, idx) =>
          val v = result.get[rt.T](idx)
          if (v != null) {
            val size = rt.storable.write(buffer, 0, v: ID[rt.T])
            val a = Array.ofDim[Byte](size)
            buffer.getBytes(0, a)
            a
          } else {
            Array.empty[Byte]
          }
      }
      rows += 1
      seq = seq :+ ResultRow(id, bytes)
      i += 1
    }
    seq
  }

  private def createFooter(result: Result): ResultFooter = {
    new ResultFooter(id, -1, rows)
  }
}
