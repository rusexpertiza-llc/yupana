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
import io.netty.buffer.ByteBuf
import org.yupana.api.query.Result
import org.yupana.protocol.{ ErrorMessage, Frame, Next, Response, ResultFooter, ResultRow, Tags }

class Streaming(context: ServerContext, result: Result) extends ConnectionState {

  private var rows = 0
  private val resultTypes = result.dataTypes.zipWithIndex
  override def init(): Seq[Response[_]] = Nil

  override def handleFrame(frame: Frame): Either[ErrorMessage, (ConnectionState, Seq[Response[_]])] = {
    import NettyBuffer._

    frame.frameType match {
      case Tags.CANCEL => Right((new Ready(context), Nil))
      case Tags.NEXT =>
        val next = Next.readFrame[ByteBuf](frame)
        val batch = createBatch(result, next.batchSize)
        if (result.hasNext) {
          Right((this, batch))
        } else {
          Right((new Ready(context), batch :+ createFooter(result)))
        }
      case x => Right((new Ready(context), Seq(ErrorMessage(s"Unexpected command ${x.toChar}"))))
    }
  }

  private def createBatch(result: Result, size: Int): Seq[ResultRow] = {
    result
      .take(size)
      .map { row =>
        val bytes = resultTypes.map {
          case (rt, idx) =>
            val v = row.get[rt.T](idx)
            if (v != null) rt.storable.write(v) else Array.empty[Byte]
        }
        rows += 1
        ResultRow(bytes)
      }
      .toSeq
  }

  private def createFooter(result: Result): ResultFooter = {
    new ResultFooter(-1, rows)
  }
}
