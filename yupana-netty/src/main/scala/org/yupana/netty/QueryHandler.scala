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
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import org.yupana.api.query.Result
import org.yupana.core.auth.YupanaUser
import org.yupana.core.sql.parser
import org.yupana.protocol._

class QueryHandler(serverContext: ServerContext, user: YupanaUser) extends FrameHandlerBase with StrictLogging {

  private var streams: Map[Int, Stream] = Map.empty

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    streams.foreach(_._2.close())
    super.channelInactive(ctx)
  }

  override def channelRead0(ctx: ChannelHandlerContext, frame: Frame[ByteBuf]): Unit = {
    frame.frameType match {
      case Tags.SQL_QUERY.value   => processMessage(ctx, frame, SqlQuery)(pq => handleQuery(ctx, pq))
      case Tags.BATCH_QUERY.value => processMessage(ctx, frame, BatchQuery)(bq => handleBatchQuery(ctx, bq))
      case Tags.NEXT.value        => processMessage(ctx, frame, NextBatch)(n => handleNext(ctx, n))
      case Tags.CANCEL.value      => processMessage(ctx, frame, Cancel)(c => cancelStream(ctx, c))
      case Tags.HEARTBEAT.value   => processMessage(ctx, frame, Heartbeat)(h => logger.debug(s"Got heartbeat $h"))
      case Tags.QUIT.value =>
        logger.info("Got quit message")
        ctx.close()
      case x => writeResponse(ctx, ErrorMessage(s"Unexpected command '${x.toChar}'"))
    }
  }

  private def handleQuery(ctx: ChannelHandlerContext, pq: SqlQuery): Unit = {
    logger.debug(s"""Processing SQL query (id: ${pq.id}): "${pq.query}"; parameters: ${pq.params}""")
    val params = pq.params.map { case (index, p) => index -> convertValue(p) }
    serverContext.queryEngineRouter.query(user, pq.query, params) match {
      case Right(result) => addStream(ctx, pq.id, result)
      case Left(msg)     => writeResponse(ctx, ErrorMessage(msg, Some(pq.id)))
    }
  }

  private def handleBatchQuery(ctx: ChannelHandlerContext, bq: BatchQuery): Unit = {
    logger.debug(s"""Processing batch SQL query (id: ${bq.id}): "${bq.query}"; parameters: ${bq.params}""")
    val params = bq.params.map(_.map { case (index, p) => index -> convertValue(p) })
    serverContext.queryEngineRouter.batchQuery(user, bq.query, params) match {
      case Right(result) => addStream(ctx, bq.id, result)
      case Left(msg)     => writeResponse(ctx, ErrorMessage(msg, Some(bq.id)))
    }
  }

  private def addStream(ctx: ChannelHandlerContext, id: Int, result: Result): Unit = {
    synchronized {
      if (!streams.contains(id)) {
        streams += id -> new Stream(id, result)
        writeResponse(ctx, makeHeader(id, result))
      } else {
        writeResponse(ctx, ErrorMessage(s"ID $id already used"))
      }
    }
  }

  private def handleNext(ctx: ChannelHandlerContext, next: NextBatch): Unit = {
    logger.debug(s"Next acquired $next")
    streams.get(next.id) match {
      case Some(stream) =>
        writeResponses(ctx, stream.next(next.batchSize))
        if (!stream.hasNext) {
          streams.synchronized {
            streams -= next.id
          }
        }
      case None => writeResponse(ctx, ErrorMessage(s"Unknown stream id ${next.id}", Some(next.id)))
    }
  }

  private def cancelStream(ctx: ChannelHandlerContext, cancel: Cancel): Unit = {
    logger.debug(s"Cancel stream $cancel")
    synchronized {
      streams.get(cancel.id) match {
        case Some(s) =>
          streams -= cancel.id
          s.close()
          writeResponse(ctx, Canceled(cancel.id))
        case None => writeResponse(ctx, ErrorMessage(s"Unknown stream id ${cancel.id}", Some(cancel.id)))
      }
    }
  }

  private def makeHeader(id: Int, result: Result): ResultHeader = {
    val resultFields = result.fieldNames.zip(result.dataTypes).map {
      case (name, rt) => ResultField(name, rt.meta.sqlTypeName)
    }
    ResultHeader(id, result.name, resultFields)
  }

  private def convertValue(value: ParameterValue): parser.Value = {
    value match {
      case StringValue(s)    => parser.StringValue(s)
      case NumericValue(n)   => parser.NumericValue(n)
      case TimestampValue(t) => parser.TimestampValue(t)
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    logger.error("Exception caught", cause)
    writeResponse(
      ctx,
      ErrorMessage(s"Cannot handle the request, '${cause.getMessage}'", None, ErrorMessage.SEVERITY_FATAL)
    )
  }
}
