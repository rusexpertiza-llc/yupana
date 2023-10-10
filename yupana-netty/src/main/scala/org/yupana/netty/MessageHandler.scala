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
import org.yupana.netty.protocol.Command

class MessageHandler(major: Int, minor: Int, version: String)
    extends SimpleChannelInboundHandler[Frame]
    with StrictLogging {

  private var state: ConnectionState = new Connecting()
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

  private def handleCommand(ctx: ChannelHandlerContext, command: Command): Unit = {
    val (newState, responses) = state.processCommand(command)
    responses.foreach { msg =>
      logger.debug(s"Write response $msg")
      ctx.write(Frame(1.toByte, msg.toFrame))
    }
    ctx.flush()
    state = newState
  }
}
