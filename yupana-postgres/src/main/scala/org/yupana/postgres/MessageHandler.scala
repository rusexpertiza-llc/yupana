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
import org.yupana.api.types._
import org.yupana.api.utils.CollectionUtils
import org.yupana.core.auth.YupanaUser
import org.yupana.core.sql.parser._
import org.yupana.core.{ EmptyQuery, PreparedCommand, PreparedSelect, PreparedStatement }
import org.yupana.postgres.MessageHandler.{ Parsed, Portal }
import org.yupana.postgres.protocol._

import java.nio.charset.Charset

class MessageHandler(context: PgContext, user: YupanaUser, charset: Charset)
    extends SimpleChannelInboundHandler[ClientMessage]
    with StrictLogging {

  implicit private val rw: ByteReaderWriter[ByteBuf] = new PostgresBinaryReaderWriter(charset)
  implicit private val srw: StringReaderWriter = PostgresStringReaderWriter

  private var parseds: Map[String, Parsed] = Map.empty
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
        ctx.write(makeDescription(result))
        writeResult(ctx, result, IndexedSeq.empty)
      case Left(error) =>
        writeError(ctx, error)
    }
    ctx.write(ReadyForQuery)
    ctx.flush()
  }

  def findTypes(ts: Seq[Int]): Either[String, Seq[Option[DataType]]] = {
    CollectionUtils.collectErrors(ts.map(t => if (t != 0) PgTypes.typeForPg(t).map(Some(_)) else Right(None)))
  }

  private def parse(sql: String): Either[String, Statement] = {
    if (sql.nonEmpty) context.queryEngineRouter.parse(sql) else Right(Select(None, SqlFieldsAll, None, Nil, None, None))
  }

  private def preprocess(sql: String): String = {
    if (sql.startsWith("SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME")) {
      "SHOW TABLES"
//    } else if (sql.startsWith("SELECT * FROM (SELECT n.nspname,c.relname,a.attname,a.atttypid,a.attnotnull")) {
//      val condIndex = sql.indexOf("WHERE")
//      if (condIndex != -1) {
//        val tablePattern = "relname LIKE E'([^']+)'".r
//        val cond = sql.substring(condIndex)
//        tablePattern.findFirstMatchIn(cond).map(_.group(1)) match {
//          case Some(table) => s"SHOW COLUMNS from $table"
//          case _           => sql
//        }
//      } else sql
    } else sql
  }

  private def parse(ctx: ChannelHandlerContext, name: String, sql: String, types: Seq[Int]): Unit = {
    val parsed = for {
      statement <- parse(preprocess(sql))
      dts <- findTypes(types)
    } yield Parsed(name, statement, dts)

    parsed match {
      case Right(p) =>
        parseds += name -> p
        ctx.write(ParseComplete)

      case Left(error) =>
        parseds -= name
        portals = portals.filterNot(_._2.parsed.name == name)
        failedStatements += name
        writeError(ctx, error)
    }
  }

  private def bind(statement: Statement, values: Map[Int, Value]): Either[String, PreparedStatement] = {
    statement match {
      case Select(None, SqlFieldsAll, None, Nil, None, None) => Right(EmptyQuery)
      case x                                                 => context.queryEngineRouter.bind(x, values)
    }

  }

  private def isBinary(idx: Int, binary: IndexedSeq[Boolean]): Boolean = {
    if (binary.length == 1) binary.head
    else if (idx < binary.length) binary(idx)
    else true
  }

  private def bind(
      ctx: ChannelHandlerContext,
      portal: String,
      prepare: String,
      paramIsBinary: IndexedSeq[Boolean],
      paramCount: Int,
      data: ByteBuf
  ): Unit = {
    parseds.get(prepare) match {
      case Some(p) =>
        if (paramCount < p.types.size) {
          logger.warn(s"Bind has only $paramCount parameters, but ${p.types.size} is required")
        }
        val values = p.types
          .take(paramCount)
          .zipWithIndex
          .map {
            case (dt, idx) =>
              val b = isBinary(idx, paramIsBinary)
              idx + 1 -> readValue(data, b, dt.map(_.aux))
          }
          .toMap

        logger.debug(s"Binding $values")
        val resultFormatCount = data.readShort()
        val resultIsBinary = (0 until resultFormatCount).map(_ => data.readShort() == 1)

        bind(p.statement, values) match {
          case Right(prep) =>
            portals += portal -> Portal(p, prep, resultIsBinary)
            ctx.write(BindComplete)
          case Left(err) =>
            writeError(ctx, err)
            failedPortals += portal
        }

      case None =>
        failedPortals += portal
        if (!failedStatements.contains(prepare))
          writeError(ctx, s"Unknown prepare $prepare")
    }
    ReferenceCountUtil.release(data)
  }

  private def readValue(in: ByteBuf, isBinary: Boolean, dataType: Option[DataType]): Value = {
    dataType match {
      case Some(dt) =>
        val v = if (isBinary) {
          dt.storable.read(in)
        } else {
          val s = implicitly[Storable[String]].read(in)
          dt.storable.readString(s)
        }
        TypedValue(v)(dt)
      case None =>
        val s = implicitly[Storable[String]].read(in)
        UntypedValue(s)
    }
  }

  private def describeStatement(ctx: ChannelHandlerContext, name: String): Unit = {}

  private def describePortal(ctx: ChannelHandlerContext, name: String): Unit = {
    portals.get(name) match {
      case Some(p) =>
        p.prepared match {
          case PreparedSelect(query) =>
            val desc = RowDescription(query.fields.map(f => fieldDesc(f.name, f.expr.dataType)).toList)
            ctx.write(desc)
          case PreparedCommand(_, _, names, types) =>
            val desc = RowDescription(names.zip(types).map { case (n, t) => fieldDesc(n, t) }.toList)
            ctx.write(desc)
          case _ => ctx.write(NoData)
        }
      case None if !failedPortals.contains(name) => writeError(ctx, s"Unknown portal $name")
      case _                                     =>
    }
  }

  private def fieldDesc(name: String, tpe: DataType): RowDescription.Field = {
    RowDescription
      .Field(
        name,
        0,
        0,
        PgTypes.pgForType(tpe),
        tpe.meta.displaySize.toShort,
        -1,
        if (PgTypes.isBinary(tpe)) 1 else 0
      )
  }

  private def execute(ctx: ChannelHandlerContext, portal: String, limit: Int): Unit = {
    portals.get(portal) match {
      case Some(p) =>
        p.parsed.statement match {
          case SetValue(_, _) => ctx.write(CommandComplete("SET"))
          case _ =>
            if (p.prepared != EmptyQuery) {
              context.queryEngineRouter.execute(user, p.prepared) match {
                case Right(result) =>
                  writeResult(ctx, result, p.resultIsBinary)
                  ctx.flush()
                case Left(error) => writeError(ctx, error)
              }
            } else {
              ctx.write(EmptyQueryResponse)
            }
        }
      case None if !failedPortals.contains(portal) => writeError(ctx, s"Unknown portal $portal")
      case _                                       => // do nothing, portal is failed
    }
  }

  private def writeResult(ctx: ChannelHandlerContext, r: Result, resultIsBinary: IndexedSeq[Boolean]): Unit = {
    var count = 0

    val resultTypes = r.dataTypes.zipWithIndex

    r.foreach { r =>
      ctx.write(makeRow(resultTypes, r, resultIsBinary))
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
        .map { case (n, t) => fieldDesc(n, t) }
        .toList
    )
  }

  private def makeRow(types: Seq[(DataType, Int)], row: DataRow, resultIsBinary: IndexedSeq[Boolean]): RowData = {

    val bufs = types.map {
      case (dt, idx) =>
        val buf = Unpooled.buffer()

        if (row.isEmpty(idx)) {
          buf.writeInt(-1)
        } else {
          if (isBinary(idx, resultIsBinary) && PgTypes.isBinary(dt)) {
            dt.storable.write(buf, row.get[dt.T](idx): ID[dt.T])
          } else {
            val s = dt.storable.writeString(row.get[dt.T](idx): ID[dt.T])
            implicitly[Storable[String]].write(buf, s: ID[String])
          }
        }
        buf
    }
    RowData(bufs)
  }
}

object MessageHandler {
  private case class Parsed(name: String, statement: Statement, types: Seq[Option[DataType]])
  private case class Portal(parsed: Parsed, prepared: PreparedStatement, resultIsBinary: IndexedSeq[Boolean])
}
