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
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.timeout.{ IdleState, IdleStateEvent, IdleStateHandler }
import org.yupana.protocol._

class ConnectingHandler(context: ServerContext) extends FrameHandlerBase with StrictLogging {

  private var gotHello = false
  override def channelRead0(ctx: ChannelHandlerContext, frame: Frame): Unit = {
    if (!gotHello) {
      handleHello(ctx, frame)
    } else {
      handleCredentials(ctx, frame)
    }
  }

  override def userEventTriggered(ctx: ChannelHandlerContext, evt: Any): Unit = {
    evt match {
      case e: IdleStateEvent if e.state() == IdleState.READER_IDLE =>
        logger.info(s"Waiting for commands from ${ctx.channel().remoteAddress()} too long")
        ctx.close()
      case _ =>
    }
  }

  private def handleHello(ctx: ChannelHandlerContext, frame: Frame): Unit = {
    processMessage(ctx, frame, Hello) {
      case Hello(pv, _, time, _) if pv == ProtocolVersion.value =>
        writeResponses(
          ctx,
          Seq(HelloResponse(ProtocolVersion.value, time), CredentialsRequest(context.authorizer.method))
        )
        gotHello = true

      case Hello(pv, _, _, _) =>
        writeResponse(
          ctx,
          ErrorMessage(
            s"Unsupported protocol version $pv, required ${ProtocolVersion.value}",
            ErrorMessage.SEVERITY_FATAL
          )
        )
    }
  }

  private def handleCredentials(
      ctx: ChannelHandlerContext,
      frame: Frame
  ): Unit = {
    processMessage(ctx, frame, Credentials) {
      case Credentials(m, u, p) =>
        context.authorizer.authorize(m, u, p) match {
          case Right(user) =>
            writeResponse(ctx, Authorized())
            connected(ctx, user)
          case Left(err) =>
            writeResponse(ctx, ErrorMessage(err, ErrorMessage.SEVERITY_FATAL))
            ctx.close()
        }
    }
  }

  private def connected(ctx: ChannelHandlerContext, userName: String): Unit = {
    if (ctx.pipeline().get(classOf[IdleStateHandler]) != null) {
      ctx.pipeline().replace(classOf[IdleStateHandler], "idleState", new IdleStateHandler(0, 60, 0))
    }
    ctx
      .pipeline()
      .replace(classOf[ConnectingHandler], "queryHandler", new QueryHandler(context, userName))

    writeResponse(ctx, Idle())
  }
}
