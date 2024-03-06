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
import org.yupana.core.auth.{ TsdbRole, YupanaUser }
import org.yupana.postgres.protocol._

import java.nio.charset.Charset

class ConnectingHandler(context: PgContext) extends SimpleChannelInboundHandler[ClientMessage] with StrictLogging {
  override def channelRead0(ctx: ChannelHandlerContext, msg: ClientMessage): Unit = {
    logger.info(s"GOT A MESSAGE $msg")
    msg match {
      case SSLRequest => ctx.writeAndFlush(No)
      case StartupMessage(user, charset) =>
        connected(ctx, user, charset)
        ctx.write(AuthOk)
        ctx.writeAndFlush(ReadyForQuery)
      case _ => ???
    }
  }

  def connected(ctx: ChannelHandlerContext, user: String, charset: Charset): Unit = {
    logger.info(s"User $user connected")

    ctx.pipeline().replace(classOf[InitialMessageDecoder], "decoder", new MessageDecoder(charset))
    ctx.pipeline().replace("encoder", "encoder", new MessageEncoder(charset))
    ctx
      .pipeline()
      .replace(
        classOf[ConnectingHandler],
        "messageHandler",
        new MessageHandler(context, YupanaUser(user, None, TsdbRole.ReadWrite), charset: Charset)
      )
  }
}
