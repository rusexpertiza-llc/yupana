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

package org.yupana.postgres

import com.typesafe.scalalogging.StrictLogging
import io.netty.channel.{ ChannelHandlerContext, SimpleChannelInboundHandler }
import org.yupana.core.auth.YupanaUser
import org.yupana.postgres.protocol._

import java.nio.charset.Charset
import scala.util.Try

class ConnectingHandler(context: PgContext) extends SimpleChannelInboundHandler[ClientMessage] with StrictLogging {

  private var userName: Option[String] = None
  private var charsetName: Option[String] = None
  private var charset: Option[Charset] = None

  override def channelRead0(ctx: ChannelHandlerContext, msg: ClientMessage): Unit = {
    logger.info(s"GOT A MESSAGE $msg")
    msg match {
      case SSLRequest => ctx.writeAndFlush(No)
      case StartupMessage(user, charsetName) =>
        setup(ctx, user, charsetName)

      case PasswordMessage(password) =>
        if (userName.isDefined) {
          context.authorizer.authorize(userName, Some(password)) match {
            case Right(user) => connected(ctx, user)
            case Left(error) => fatalError(ctx, error)
          }
        } else {
          fatalError(ctx, "Got password but user is undefined yet")
        }

      case _ => fatalError(ctx, s"Unexpected message $msg")
    }
  }

  private def fatalError(ctx: ChannelHandlerContext, msg: String): Unit = {
    ctx.writeAndFlush(ErrorResponse(msg))
    ctx.close()
  }

  private def setup(ctx: ChannelHandlerContext, user: String, charsetName: String): Unit = {
    this.userName = Some(user)
    this.charsetName = Some(charsetName)
    this.charset = Try(Charset.forName(charsetName)).toOption
    if (charset.isDefined) {
      ctx.pipeline().replace(classOf[InitialMessageDecoder], "decoder", new MessageDecoder(charset.get))
      ctx.writeAndFlush(AuthClearTextPassword)
    } else {
      fatalError(ctx, s"Unsupported charset $charsetName")
    }
  }

  private def connected(ctx: ChannelHandlerContext, user: YupanaUser): Unit = {
    logger.info(s"User ${user.name} connected")

    ctx.write(AuthOk)
    ctx.write(ParameterStatus("client_encoding", charsetName.get))
    ctx.write(ParameterStatus("is_superuser", "off"))
    ctx.write(ParameterStatus("server_version", "9.0.0"))
    ctx.write(ParameterStatus("session_authorization", user.name))
    ctx.writeAndFlush(ReadyForQuery)

    ctx.pipeline().replace("encoder", "encoder", new MessageEncoder(charset.get))
    ctx
      .pipeline()
      .replace(
        classOf[ConnectingHandler],
        "messageHandler",
        new MessageHandler(context, user, charset.get)
      )
  }
}
