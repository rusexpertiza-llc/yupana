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

import com.typesafe.scalalogging.Logger
import io.netty.buffer.{ ByteBuf, Unpooled }
import io.netty.channel.{ ChannelHandlerContext, SimpleChannelInboundHandler }
import org.yupana.api.types.ByteReaderWriter
import org.yupana.protocol.{ ErrorMessage, Frame, Message, MessageHelper, Response }

abstract class FrameHandlerBase extends SimpleChannelInboundHandler[Frame[ByteBuf]] {

  protected def logger: Logger

  implicit private val rw: ByteReaderWriter[ByteBuf] = ByteBufEvalReaderWriter

  def readMessage[M <: Message[M]](f: Frame[ByteBuf], helper: MessageHelper[M]): Either[ErrorMessage, M] = {
    helper.readFrameOpt(f).toRight(ErrorMessage(s"Expect '${helper.tag.value.toChar}' but got '${f.frameType}'"))
  }
  def processMessage[M <: Message[M]](ctx: ChannelHandlerContext, frame: Frame[ByteBuf], helper: MessageHelper[M])(
      f: M => Unit
  ): Unit = {
    readMessage(frame, helper).fold(writeResponse(ctx, _), f)
  }

  def writeResponse(ctx: ChannelHandlerContext, response: Response[_]): Unit = {
    logger.trace(s"Write response $response")
    ctx.writeAndFlush(response.toFrame(Unpooled.buffer()))
  }

  def writeResponses(ctx: ChannelHandlerContext, responses: Seq[Response[_]]): Unit = {
    responses.foreach { response =>
      logger.trace(s"Write response $response")
      ctx.write(response.toFrame(Unpooled.buffer()))
    }
    ctx.flush()
  }

  def writeError(
      ctx: ChannelHandlerContext,
      msg: String,
      streamId: Option[Int] = None,
      severity: Byte = ErrorMessage.SEVERITY_ERROR
  ): Unit = {
    val error = if (severity == ErrorMessage.SEVERITY_ERROR) "Error" else "Fatal error"
    streamId match {
      case Some(id) => logger.warn(s"$error '$msg' in stream $id'")
      case None     => logger.warn(s"$error '$msg'")
    }
    val response = ErrorMessage(msg, streamId, severity)
    ctx.writeAndFlush(response.toFrame(Unpooled.buffer()))
  }
}
