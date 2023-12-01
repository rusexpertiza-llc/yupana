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

import org.yupana.jdbc.FramingChannelReader.PAYLOAD_OFFSET
import org.yupana.protocol.Frame

import java.io.IOException
import java.nio.channels.{ AsynchronousByteChannel, CompletionHandler }
import java.nio.{ ByteBuffer, ByteOrder }
import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, Future, Promise }

class FramingChannelReader(
    channel: AsynchronousByteChannel,
    maxFrameSize: Int
) {

  private val buffer = createBuffer

  private def createBuffer = {
    val buf = ByteBuffer.allocate(maxFrameSize).order(ByteOrder.BIG_ENDIAN)
    buf.clear()
    buf
  }

  final def readFrame(): Future[Frame] = {
    buffer.synchronized {
      val p = Promise[Frame]()

      channel.read(
        buffer,
        null,
        new CompletionHandler[Integer, Any] {
          override def completed(r: Integer, attachment: Any): Unit = {
            if (r == -1 && buffer.position() < PAYLOAD_OFFSET) {
              p.failure(new IOException("Unexpected end of response"))
            } else {
//              if (buffer.position() >= PAYLOAD_OFFSET) {
              val tag = buffer.get(0)
              val size = buffer.getInt(1)
              var lastRead = 0
              while (buffer.position() < size + PAYLOAD_OFFSET && lastRead >= 0) {
                lastRead = channel.read(buffer).get()
                //            if (lastRead == 0) Thread.sleep(1)
              }
              val result = Array.ofDim[Byte](size)
              if (buffer.position() < size + PAYLOAD_OFFSET) {
                p.failure(new IOException("Unexpected end of response"))
              } else {
                val totalRead = buffer.position()

                buffer.position(PAYLOAD_OFFSET)
                buffer.get(result)

                val restSize = totalRead - PAYLOAD_OFFSET - size
                System.arraycopy(buffer.array(), buffer.position(), buffer.array(), 0, restSize)
                buffer.position(restSize)

                p.success(Frame(tag, result))
              }
//              } else {
//                None
//              }
            }
          }

          override def failed(exc: Throwable, attachment: Any): Unit = p.failure(exc)
        }
      )
      p.future
    }
  }

  final def awaitAndReadFrame(): Frame = {
    Await.result(readFrame(), Duration.Inf)
  }
}

object FramingChannelReader {
  val PAYLOAD_OFFSET: Int = 1 + 4 // TAG: Byte || SIZE: Int || PAYLOAD
}
