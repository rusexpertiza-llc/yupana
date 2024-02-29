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
import org.yupana.core.auth.YupanaUser
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
          Seq(HelloResponse(ProtocolVersion.value, time), CredentialsRequest(ConnectingHandler.SUPPORTED_METHODS))
        )
        gotHello = true

      case Hello(pv, _, _, _) =>
        respondFatal(ctx, s"Unsupported protocol version $pv, required ${ProtocolVersion.value}")
        ctx.close()
    }
  }

  private def handleCredentials(
      ctx: ChannelHandlerContext,
      frame: Frame
  ): Unit = {
    processMessage(ctx, frame, Credentials) {
      case Credentials(m, u, p) =>
        if (ConnectingHandler.SUPPORTED_METHODS.contains(m)) {
          context.authorizer.authorize(u, p) match {
            case Right(user) =>
              connected(ctx, user)
              writeResponse(ctx, Authorized())
            case Left(err) =>
              respondFatal(ctx, err)
          }
        } else {
          respondFatal(ctx, s"Unsupported auth method '$m'")
        }
    }
  }

  private def connected(ctx: ChannelHandlerContext, user: YupanaUser): Unit = {
    logger.info(s"User ${user.name} connected")
    if (ctx.pipeline().get(classOf[IdleStateHandler]) != null) {
      ctx.pipeline().remove(classOf[IdleStateHandler])
    }
    ctx
      .pipeline()
      .replace(classOf[ConnectingHandler], "queryHandler", new QueryHandler(context, user))
  }

  private def respondFatal(ctx: ChannelHandlerContext, message: String): Unit = {
    logger.info(s"Failed to connect: '$message'")
    writeResponse(ctx, ErrorMessage(message, None, ErrorMessage.SEVERITY_FATAL))
    ctx.close()
  }

}

object ConnectingHandler {
  val SUPPORTED_METHODS: List[String] = List(CredentialsRequest.METHOD_PLAIN)
}
