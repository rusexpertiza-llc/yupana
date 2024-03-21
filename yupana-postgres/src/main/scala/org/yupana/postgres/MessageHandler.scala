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
import io.netty.util.ReferenceCountUtil
import org.yupana.api.query.{ DataRow, Result }
import org.yupana.api.types.{ ByteReaderWriter, DataType, ID }
import org.yupana.core.auth.YupanaUser
import org.yupana.core.sql.parser.{ SetValue, Statement, Value }
import org.yupana.postgres.MessageHandler.{ Portal, Prepared }
import org.yupana.postgres.protocol._

import java.nio.charset.Charset

class MessageHandler(context: PgContext, user: YupanaUser, charset: Charset)
    extends SimpleChannelInboundHandler[ClientMessage]
    with StrictLogging {

  implicit private val rw: ByteReaderWriter[ByteBuf] = new PostgresBinaryReaderWriter(charset)

  private var prepareds: Map[String, Prepared] = Map.empty
  private var failedStatements: Set[String] = Set.empty
  private var portals: Map[String, Portal] = Map.empty
  private var failedPortals: Set[String] = Set.empty

  override def channelRead0(ctx: ChannelHandlerContext, msg: ClientMessage): Unit = {
    logger.debug(s"Handle message $msg")
    msg match {
      case SimpleQuery(sql)                                  => simpleQuery(ctx, sql)
      case Parse(name, sql, types)                           => parse(ctx, name, sql, types)
      case Bind(portal, prepare, isBinary, paramCount, data) => bind(ctx, portal, prepare, isBinary, paramCount, data)
      case Describe(Describe.DescribeStatement, n)           => describeStatement(ctx, n)
      case Describe(Describe.DescribePortal, n)              => describePortal(ctx, n)
      case Execute(name, limit)                              => execute(ctx, name, limit)

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
      case Left(error) =>
        writeError(ctx, error)
    }
    ctx.write(ReadyForQuery)
    ctx.flush()
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
        failedStatements += name
        writeError(ctx, error)
    }
  }

  private def bind(
      ctx: ChannelHandlerContext,
      portal: String,
      prepare: String,
      isBinary: Seq[Boolean],
      paramCount: Short,
      data: ByteBuf
  ): Unit = {
    prepareds.get(prepare) match {
      case Some(p) =>
        portals += portal -> Portal(p, Map.empty)
        val values = p.types.zipWithIndex.map {
          case (dt, idx) =>
            val b = if (isBinary.length == 1) isBinary.head else if (idx < isBinary.length) isBinary(idx) else true
            readValue(data, b, dt)
        }

        println(s"VALUES: $values")
        if (p.types.nonEmpty) {

          writeError(ctx, "Bind not ready yet")
        } else {
          ctx.write(BindComplete)
        }
      case None =>
        failedPortals += portal
        if (!failedStatements.contains(prepare))
          writeError(ctx, s"Unknown prepare $prepare")
    }
    ReferenceCountUtil.release(data)
  }

  private def readValue(in: ByteBuf, isBinary: Boolean, dt: DataType): Any = {
    dt.storable.read(in)
  }

  private def describeStatement(ctx: ChannelHandlerContext, name: String): Unit = {}

  private def describePortal(ctx: ChannelHandlerContext, name: String): Unit = {}

  private def execute(ctx: ChannelHandlerContext, portal: String, limit: Int): Unit = {
    portals.get(portal) match {
      case Some(p) =>
        p.prepared.statement match {
          case SetValue(_, _) =>
            ctx.write(CommandComplete("SET"))
          case s =>
            context.queryEngineRouter.execute(user, s, p.values) match {
              case Right(result) =>
                writeResult(ctx, result)
                ctx.flush()
              case Left(error) => writeError(ctx, error)
            }
        }
      case None if !failedPortals.contains(portal) => writeError(ctx, s"Unknown portal $portal")
      case _                                       => // do nothing, portal is failed
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

object MessageHandler {
  private case class Prepared(statement: Statement, types: Seq[DataType])
  private case class Portal(prepared: Prepared, values: Map[Int, Value])
}
