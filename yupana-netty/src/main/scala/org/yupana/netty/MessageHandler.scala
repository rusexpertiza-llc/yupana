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
import org.yupana.protocol.{ Command, Frame, Response }

class MessageHandler(major: Int, minor: Int, version: String)
    extends SimpleChannelInboundHandler[Frame]
    with StrictLogging {

  private var state: ConnectionState = new Connecting()

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    writeResponses(ctx, state.init())
  }

  override def channelRead0(ctx: ChannelHandlerContext, frame: Frame): Unit = {
    println(s"GOT A FRAME ${frame.frameType}")
    state.extractCommand(frame) match {
      case Right(Some(cmd)) => handleCommand(ctx, cmd)
      case Right(None)      => logger.debug(s"Ignoring command with type: ${frame.frameType}")

      case Left(err) =>
        logger.error(err.message)
        ctx.write(err)
    }
  }

  private def handleCommand(ctx: ChannelHandlerContext, command: Command[_]): Unit = {
    val (newState, responses) = state.processCommand(command)
    writeResponses(ctx, responses)
    state = newState
    val initial = state.init()
    if (initial.nonEmpty) {
      writeResponses(ctx, initial)
    }
  }

  private def writeResponses(ctx: ChannelHandlerContext, responses: Seq[Response[_]]): Unit = {
    responses.foreach { msg =>
      logger.debug(s"Write response $msg")
      ctx.write(msg.toFrame)
    }
    ctx.flush()
  }
}
