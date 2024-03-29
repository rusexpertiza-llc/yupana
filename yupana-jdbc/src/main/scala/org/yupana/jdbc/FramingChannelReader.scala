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

import java.io.IOException
import java.nio.channels.ReadableByteChannel
import java.nio.{ ByteBuffer, ByteOrder }
import scala.annotation.tailrec

class FramingChannelReader(
    channel: ReadableByteChannel,
    maxFrameSize: Int
) {

  private val buffer = createBuffer

  private def createBuffer = {
    val buf = ByteBuffer.allocate(maxFrameSize).order(ByteOrder.BIG_ENDIAN)
    buf.clear()
    buf
  }

  final def readFrame(): Option[Array[Byte]] = {
    buffer.synchronized {
      try {
        val r = channel.read(buffer)
        if (r == -1 && buffer.position() < 4) throw new IOException("Unexpected end of response")
        if (buffer.position() >= 4) {
          val size = buffer.getInt(0)
          var lastRead = 0
          while (buffer.position() < size + 4 && lastRead >= 0) {
            lastRead = channel.read(buffer)
            if (lastRead == 0) Thread.sleep(1)
          }
          val result = Array.ofDim[Byte](size)
          if (buffer.position() < size + 4) throw new IOException("Unexpected end of response")
          val totalRead = buffer.position()

          buffer.position(4)
          buffer.get(result)

          val restSize = totalRead - 4 - size
          System.arraycopy(buffer.array(), buffer.position(), buffer.array(), 0, restSize)
          buffer.position(restSize)

          Some(result)
        } else {
          None
        }
      } catch {
        case t: Throwable =>
          if (channel.isOpen) channel.close()
          throw t
      }
    }
  }

  @tailrec
  final def awaitAndReadFrame(): Array[Byte] = {
    readFrame() match {
      case Some(r) => r
      case None =>
        Thread.sleep(1)
        awaitAndReadFrame()
    }
  }
}
