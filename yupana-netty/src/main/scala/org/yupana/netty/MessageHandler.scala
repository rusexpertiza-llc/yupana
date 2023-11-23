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

import com.typesafe.scalalogging.StrictLogging
import io.netty.channel.{ ChannelHandlerContext, SimpleChannelInboundHandler }
import org.yupana.protocol.{ ErrorMessage, Frame, Message, MessageHelper, Response }

class MessageHandler(serverContext: ServerContext) extends SimpleChannelInboundHandler[Frame] with StrictLogging {

  private var state: ConnectionState = new Connecting(serverContext)

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    writeResponses(ctx, state.init())
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    state.close()
    super.channelInactive(ctx)
  }

  override def channelRead0(ctx: ChannelHandlerContext, frame: Frame): Unit = {
    println(s"MH HAVE FRAME ${frame.frameType.toChar}")
    state.handleFrame(frame) match {
      case Right((newState, responses)) =>
        writeResponses(ctx, responses)
        if (state != newState) {
          state = newState
          val initial = state.init()
          if (initial.nonEmpty) {
            writeResponses(ctx, initial)
          }
        }

      case Left(err) =>
        logger.error(err.message)
        ctx.writeAndFlush(err.toFrame)
        if (err.severity == ErrorMessage.SEVERITY_FATAL) ctx.close()
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    super.exceptionCaught(ctx, cause)
    ctx.writeAndFlush(ErrorMessage(s"Something goes wrong, ${cause.getMessage}", ErrorMessage.SEVERITY_FATAL))
    ctx.close()
  }

  private def writeResponses(ctx: ChannelHandlerContext, responses: Seq[Response[_]]): Unit = {
    responses.foreach { msg =>
      logger.debug(s"Write response $msg")
      ctx.write(msg.toFrame)
    }
    ctx.flush()
  }
}

object MessageHandler {
  def readMessage[M <: Message[M]](f: Frame, helper: MessageHelper[M]): Either[ErrorMessage, M] = {
    helper.readFrameOpt(f).toRight(ErrorMessage(s"Expect '${helper.tag.toChar}' but got '${f.frameType}'"))
  }
}
