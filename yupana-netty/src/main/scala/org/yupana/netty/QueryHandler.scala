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
import org.yupana.api.Time
import org.yupana.api.query.Result
import org.yupana.api.types.{ SimpleStringReaderWriter, StringReaderWriter }
import org.yupana.core.auth.YupanaUser
import org.yupana.core.sql.{ Parameter, TypedParameter }
import org.yupana.protocol._

class QueryHandler(serverContext: ServerContext, user: YupanaUser) extends FrameHandlerBase with StrictLogging {

  private var streams: Map[Int, Stream] = Map.empty
  implicit private val srw: StringReaderWriter = SimpleStringReaderWriter

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
      case x => writeError(ctx, s"Unexpected command '${x.toChar}'")
    }
    frame.payload.release()
  }

  private def handleQuery(ctx: ChannelHandlerContext, pq: SqlQuery): Unit = {
    logger.debug(s"""Processing SQL query (id: ${pq.id}): "${pq.query}"; parameters: ${pq.params}""")
    val params = pq.params.map { case (index, p) => index -> convertParameter(p) }
    try {
      serverContext.queryEngineRouter.query(user, pq.query, params) match {
        case Right(result) => addStream(ctx, pq.id, result)
        case Left(msg)     => writeError(ctx, msg, Some(pq.id))
      }
    } catch {
      case t: Throwable => failStream(ctx, pq.id, t.getMessage)
    }
  }

  private def handleBatchQuery(ctx: ChannelHandlerContext, bq: BatchQuery): Unit = {
    logger.debug(s"""Processing batch SQL query (id: ${bq.id}): "${bq.query}"; parameters: ${bq.params}""")
    val params = bq.params.map(_.map { case (index, p) => index -> convertParameter(p) })
    try {
      serverContext.queryEngineRouter.batchQuery(user, bq.query, params) match {
        case Right(result) => addStream(ctx, bq.id, result)
        case Left(msg)     => writeError(ctx, msg, Some(bq.id))
      }
    } catch {
      case t: Throwable => failStream(ctx, bq.id, t.getMessage)
    }
  }

  private def addStream(ctx: ChannelHandlerContext, id: Int, result: Result): Unit = {
    if (!streams.contains(id)) {
      streams += id -> new Stream(id, result)
      writeResponse(ctx, makeHeader(id, result))
    } else {
      writeError(ctx, s"ID $id already used")
    }
  }

  private def handleNext(ctx: ChannelHandlerContext, next: NextBatch): Unit = {
    logger.debug(s"Next acquired $next")
    streams.get(next.queryId) match {
      case Some(stream) =>
        try {
          val batch = stream.next(next.batchSize)
          writeResponses(ctx, batch)
          if (!stream.hasNext) {
            streams -= next.queryId
          }
        } catch {
          case e: Throwable => failStream(ctx, next.queryId, e.getMessage)
        }
      case None => writeError(ctx, s"Next for unknown stream id ${next.queryId}", Some(next.queryId))
    }
  }

  private def failStream(ctx: ChannelHandlerContext, queryId: Int, msg: String): Unit = {
    writeError(ctx, s"Query process failed, $msg", Some(queryId))
    streams -= queryId
  }

  private def cancelStream(ctx: ChannelHandlerContext, cancel: Cancel): Unit = {
    logger.debug(s"Cancel stream $cancel")
    streams.get(cancel.queryId) match {
      case Some(s) =>
        streams -= cancel.queryId
        s.close()
        writeResponse(ctx, Cancelled(cancel.queryId))
      case None => writeError(ctx, s"Cancel for unknown stream id ${cancel.queryId}", Some(cancel.queryId))
    }
  }

  private def makeHeader(id: Int, result: Result): ResultHeader = {
    val resultFields = result.fieldNames.zip(result.dataTypes).map {
      case (name, rt) => ResultField(name, rt.meta.sqlTypeName)
    }
    ResultHeader(id, result.name, resultFields)
  }

  private def convertParameter(value: ParameterValue): Parameter = {
    value match {
      case StringValue(s)    => TypedParameter(s)
      case NumericValue(n)   => TypedParameter(n)
      case TimestampValue(t) => TypedParameter(Time(t))
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    logger.error("Exception caught", cause)
    writeError(ctx, s"Cannot handle the request, '${cause.getMessage}'", None, ErrorMessage.SEVERITY_FATAL)
  }
}
