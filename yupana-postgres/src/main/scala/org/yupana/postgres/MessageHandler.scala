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
import io.netty.buffer.{ ByteBuf, Unpooled }
import io.netty.channel.{ ChannelHandlerContext, SimpleChannelInboundHandler }
import org.yupana.api.query.{ DataRow, Result }
import org.yupana.api.types.{ ByteReaderWriter, DataType, ID }
import org.yupana.core.auth.YupanaUser
import org.yupana.core.sql.parser.{ Statement, Value }
import org.yupana.postgres.protocol._

import java.nio.charset.Charset

case class Prepared(statement: Statement, types: Seq[DataType])

case class Portal(prepared: Prepared, values: Map[Int, Value])

class MessageHandler(context: PgContext, user: YupanaUser, charset: Charset)
    extends SimpleChannelInboundHandler[ClientMessage]
    with StrictLogging {

  implicit private val rw: ByteReaderWriter[ByteBuf] = new PostgresReaderWriter(charset)

  private var prepareds: Map[String, Prepared] = Map.empty
  private var portals: Map[String, Portal] = Map.empty

  override def channelRead0(ctx: ChannelHandlerContext, msg: ClientMessage): Unit = {
    logger.debug(s"Handle message $msg")
    msg match {
      case SimpleQuery(sql)                        => simpleQuery(ctx, sql)
      case Parse(name, sql, types)                 => parse(ctx, name, sql, types)
      case Bind(portal, prepare)                   => bind(ctx, portal, prepare)
      case Describe(Describe.DescribeStatement, n) => describeStatement(ctx, n)
      case Describe(Describe.DescribePortal, n)    => describePortal(ctx, n)
      case Execute(name, limit)                    => execute(ctx, name, limit)

      case Sync =>
        ctx.writeAndFlush(ReadyForQuery)

      case Quit =>
        ctx.close()

      case _ => writeError(ctx, s"Unsupported command '$msg'")
    }
  }

  private def simpleQuery(ctx: ChannelHandlerContext, sql: String): Unit = {
    context.queryEngineRouter.query(user, sql, Map.empty) match {
      case Right(result) =>
        writeResult(ctx, result)
        ctx.write(ReadyForQuery)
        ctx.flush()
      case Left(error) => writeError(ctx, error)
    }
  }

  private def parse(ctx: ChannelHandlerContext, name: String, sql: String, types: Seq[Int]): Unit = {
    val prepared = for {
      statement <- context.queryEngineRouter.parse(sql)
      dts <- PgTypes.findTypes(types)
    } yield Prepared(statement, dts)

    prepared match {
      case Right(p) =>
        prepareds += name -> p
        ctx.write(ParseComplete)

      case Left(error) =>
        writeError(ctx, error)
    }
  }

  private def bind(ctx: ChannelHandlerContext, portal: String, prepare: String): Unit = {
    prepareds.get(prepare) match {
      case Some(p) =>
        portals += portal -> Portal(p, Map.empty)
        if (p.types.nonEmpty) {
          writeError(ctx, "Bind not ready yet")
        } else {
          ctx.write(BindComplete)
        }
      case None => writeError(ctx, s"Unknown prepare $prepare")
    }
  }

  private def describeStatement(ctx: ChannelHandlerContext, name: String): Unit = {}

  private def describePortal(ctx: ChannelHandlerContext, name: String): Unit = {}

  private def execute(ctx: ChannelHandlerContext, portal: String, limit: Int): Unit = {
    portals.get(portal) match {
      case Some(p) =>
        context.queryEngineRouter.execute(user, p.prepared.statement, p.values) match {
          case Right(result) =>
            writeResult(ctx, result)
            ctx.flush()
          case Left(error) => writeError(ctx, error)
        }
      case None => writeError(ctx, s"Unknown portal $portal")
    }
  }

  private def writeResult(ctx: ChannelHandlerContext, r: Result): Unit = {
    ctx.write(makeDescription(r))

    var count = 0

    val resultTypes = r.dataTypes.zipWithIndex

    r.foreach { r =>
      ctx.write(makeRow(resultTypes, r))
      count += 1
    }

    ctx.write(CommandComplete(s"SELECT $count"))
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
          case (n, t) =>
            RowDescription
              .Field(n, 0, 0, PgTypes.pgForType(t), t.meta.displaySize.toShort, -1, if (PgTypes.isBinary(t)) 1 else 0)
        }
        .toList
    )
  }

  private def makeRow(types: Seq[(DataType, Int)], row: DataRow): RowData = {

    val bufs = types.map {
      case (dt, idx) =>
        val buf = Unpooled.buffer()

        if (row.isEmpty(idx)) {
          buf.writeInt(-1)
        } else {
          dt.storable.write(buf, row.get[dt.T](idx): ID[dt.T])
        }
        buf
    }
    RowData(bufs)
  }

}
