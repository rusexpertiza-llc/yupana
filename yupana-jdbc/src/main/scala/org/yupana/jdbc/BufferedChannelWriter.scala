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

import org.yupana.api.types.ByteReaderWriter
import org.yupana.protocol.{ Command, Frame }

import java.nio.ByteBuffer
import java.nio.channels.{ AsynchronousSocketChannel, CompletionHandler }
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import scala.collection.mutable
import scala.concurrent.{ Future, Promise }

class BufferedChannelWriter(channel: AsynchronousSocketChannel)(implicit writer: ByteReaderWriter[ByteBuffer]) {

  private val logger: Logger = Logger.getLogger(classOf[BufferedChannelWriter].getName)
  private var isWriting = false
  private val commandQueue = mutable.Queue.empty[(Command[_], Promise[Unit])]

  def write(command: Command[_]): Future[Unit] = {
    logger.fine(s"Writing command ${command.helper.tag.value.toChar}")
    if (!isWriting) {
      isWriting = true
      val f = command.toFrame[ByteBuffer](ByteBuffer.allocate(Frame.MAX_FRAME_SIZE))
      val bb = ByteBuffer.allocate(f.payload.position() + 4 + 1)
      bb.put(f.frameType)
      bb.putInt(f.payload.position())
      f.payload.flip()
      bb.put(f.payload)
      bb.flip()

      JdbcUtils.wrapHandler[Unit](
        new CompletionHandler[Integer, Promise[Unit]] {
          override def completed(result: Integer, p: Promise[Unit]): Unit = {
            isWriting = false
            p.success(())
          }

          override def failed(exc: Throwable, p: Promise[Unit]): Unit = {
            isWriting = false
            p.failure(exc)
          }
        },
        (p, h) => channel.write(bb, 10, TimeUnit.SECONDS, p, h)
      )
    } else {
      val p = Promise[Unit]()
      commandQueue.enqueue((command, p))
      p.future
    }
  }
}
