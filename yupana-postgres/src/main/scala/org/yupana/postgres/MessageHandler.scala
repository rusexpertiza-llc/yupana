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

import io.netty.channel.{ ChannelHandlerContext, SimpleChannelInboundHandler }
import org.yupana.api.query.Result
import org.yupana.core.auth.YupanaUser
import org.yupana.postgres.protocol.{
  ClientMessage,
  CommandComplete,
  ErrorResponse,
  PgTypes,
  ReadyForQuery,
  RowDescription,
  SimpleQuery
}

import java.nio.charset.Charset

class MessageHandler(context: PgContext, user: YupanaUser, charset: Charset)
    extends SimpleChannelInboundHandler[ClientMessage] {
  override def channelRead0(ctx: ChannelHandlerContext, msg: ClientMessage): Unit = {
    msg match {
      case SimpleQuery(sql) =>
        context.queryEngineRouter.query(user, sql, Map.empty) match {
          case Right(result) => writeResult(ctx, result)
          case Left(error)   => writeError(ctx, error)
        }

      case _ => ???
    }
  }

  private def writeResult(ctx: ChannelHandlerContext, r: Result): Unit = {
    ctx.write(makeDescription(r))

    val count = 0

    ctx.write(CommandComplete(s"SELECT $count"))
    ctx.write(ReadyForQuery)
    ctx.flush()
  }

  private def writeError(ctx: ChannelHandlerContext, message: String): Unit = {
    ctx.write(ErrorResponse(message))
    ctx.write(ReadyForQuery)
    ctx.flush()
  }

  private def makeDescription(result: Result): RowDescription = {
    RowDescription(
      result.fieldNames
        .zip(result.dataTypes)
        .map {
          case (n, t) => RowDescription.Field(n, 0, 0, PgTypes.pgForType(t), t.meta.displaySize.toShort, -1, 0)
        }
        .toList
    )
  }

}
