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

  private def extractFrame(tag: Byte, size: Int): Frame = {
    val result = Array.ofDim[Byte](size)
    val totalRead = buffer.position()

    buffer.position(PAYLOAD_OFFSET)
    buffer.get(result)

    val restSize = totalRead - PAYLOAD_OFFSET - size
    System.arraycopy(buffer.array(), buffer.position(), buffer.array(), 0, restSize)
    buffer.position(restSize)
    Frame(tag, result)
  }

  private val completionHandler = new CompletionHandler[Integer, Promise[Frame]] {
    override def completed(r: Integer, promise: Promise[Frame]): Unit = {
      if (r == -1) {
        promise.failure(new IOException("Unexpected end of response"))
      } else {
        if (buffer.position() >= PAYLOAD_OFFSET) {
          val tag = buffer.get(0)
          val size = buffer.getInt(1)
          if (buffer.position() < size + PAYLOAD_OFFSET) {
            channel.read(buffer, promise, this)
          } else {
            if (buffer.position() < size + PAYLOAD_OFFSET) {
              promise.failure(new IOException("Unexpected end of response"))
            } else {
              promise.success(extractFrame(tag, size))
            }
          }
        } else {
          channel.read(buffer, promise, this)
        }
      }
    }

    override def failed(exc: Throwable, promise: Promise[Frame]): Unit = promise.failure(exc)
  }

  final def readFrame(): Future[Frame] = {
    if (buffer.position() >= PAYLOAD_OFFSET) {
      val tag = buffer.get(0)
      val size = buffer.getInt(1)
      if (buffer.position() < size + PAYLOAD_OFFSET) {
        JdbcUtils.wrapHandler[Frame](completionHandler, (p, h) => channel.read(buffer, p, h))
      } else {
        Future.successful(extractFrame(tag, size))
      }
    } else {
      JdbcUtils.wrapHandler[Frame](completionHandler, (p, h) => channel.read(buffer, p, h))
    }
  }

  final def awaitAndReadFrame(): Frame = {
    Await.result(readFrame(), Duration.Inf)
  }
}

object FramingChannelReader {
  val PAYLOAD_OFFSET: Int = 1 + 4 // TAG: Byte || SIZE: Int || PAYLOAD
}
