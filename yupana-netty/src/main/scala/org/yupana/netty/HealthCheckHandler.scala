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
import io.netty.buffer.Unpooled
import io.netty.channel.{ ChannelFutureListener, ChannelHandlerContext, SimpleChannelInboundHandler }
import io.netty.handler.codec.http.{
  DefaultFullHttpResponse,
  HttpMethod,
  HttpRequest,
  HttpResponseStatus,
  HttpVersion,
  QueryStringDecoder
}
import io.netty.util.CharsetUtil
import org.yupana.api.types.{ SimpleStringReaderWriter, StringReaderWriter }
import org.yupana.core.auth.YupanaUser

class HealthCheckHandler(serverContext: ServerContext)
    extends SimpleChannelInboundHandler[HttpRequest]
    with StrictLogging {

  implicit private val srw: StringReaderWriter = SimpleStringReaderWriter

  override def channelRead0(ctx: ChannelHandlerContext, msg: HttpRequest): Unit = {
    val decoder = new QueryStringDecoder(msg.uri(), true)

    val response = if (msg.method() == HttpMethod.GET && decoder.path() == "/health-check") {
      logger.debug("Perform health check")
      serverContext.queryEngineRouter
        .query(YupanaUser.ANONYMOUS, "SELECT 1", Map.empty)
        .flatMap(result =>
          Either.cond(result.next() && result.get[BigDecimal](0) == 1, "Ok", "Unexpected health-check result")
        ) match {
        case Right(ok) =>
          logger.debug("Health check succeed")
          createResponse(HttpResponseStatus.OK, Some(ok))
        case Left(err) =>
          logger.error("Health check failed")
          createResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, Some(err))
      }
    } else {
      logger.warn("Unsupported request on health check port")
      createResponse(HttpResponseStatus.NOT_FOUND, None)
    }
    ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE)
  }

  private def createResponse(status: HttpResponseStatus, text: Option[String]) = {
    val content = text.map(s => Unpooled.copiedBuffer(s, CharsetUtil.UTF_8)).getOrElse(Unpooled.buffer(0))
    new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content)
  }
}
