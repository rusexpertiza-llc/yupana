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
import org.yupana.api.types.{ ByteReaderWriter, ID }
import org.yupana.protocol.{ Response, ResultFooter, ResultRow }

class Stream(id: Int, result: Result) {

  private var rows = 0
  private val resultTypes = result.dataTypes.zipWithIndex
  val MAX_VALUE_SIZE = 2_000_000
  private val buffer = Unpooled.buffer(MAX_VALUE_SIZE)
  implicit val readerWriter: ByteReaderWriter[ByteBuf] = ByteBufEvalReaderWriter

  def close(): Unit = result.close()

  def next(count: Int): Seq[Response[_]] = {

    val batch = createBatch(result, count)
    if (result.hasNext) {
      batch
    } else {
      batch :+ createFooter(result)
    }
  }

  def hasNext: Boolean = result.hasNext

  private def createBatch(result: Result, size: Int): Seq[ResultRow] = {
    result
      .take(size)
      .map { row =>
        val bytes = resultTypes.map {
          case (rt, idx) =>
            val v = row.get[rt.T](idx)
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
        ResultRow(id, bytes)
      }
      .toSeq
  }

  private def createFooter(result: Result): ResultFooter = {
    new ResultFooter(id, -1, rows)
  }
}
