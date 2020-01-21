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

package org.yupana.akka

import com.google.protobuf.ByteString
import com.typesafe.scalalogging.StrictLogging
import org.yupana.api.query.{ Query, Result, SimpleResult }
import org.yupana.api.schema.Schema
import org.yupana.api.types.DataType
import org.yupana.core.TSDB
import org.yupana.core.sql.SqlQueryProcessor
import org.yupana.core.sql.parser._
import org.yupana.proto
import org.yupana.proto.util.ProtocolVersion

import scala.concurrent.{ ExecutionContext, Future }

class RequestHandler(schema: Schema) extends StrictLogging {

  private val sqlQueryProcessor = new SqlQueryProcessor(schema)
  private val metadataProvider = new JdbcMetadataProvider(schema)

  def handleQuery(tsdb: TSDB, sqlQuery: proto.SqlQuery)(
      implicit ec: ExecutionContext
  ): Future[Either[String, Iterator[proto.Response]]] = {

    logger.debug(s"""Processing SQL query: "${sqlQuery.sql}"; parameters: ${sqlQuery.parameters}""")

    Future {

      SqlParser.parse(sqlQuery.sql).right flatMap {

        case select: Select =>
          val params = sqlQuery.parameters.map(p => p.index -> convertValue(p.value)).toMap
          val tsdbQuery: Either[String, Query] = sqlQueryProcessor.createQuery(select, params)
          tsdbQuery.right flatMap { query =>
            val rs = tsdb.query(query)
            Right(resultToProto(rs))
          }

        case upsert: Upsert =>
          val params = Seq(sqlQuery.parameters.map(p => p.index -> convertValue(p.value)).toMap)
          doUpsert(tsdb, upsert, params)

        case ShowTables => Right(resultToProto(metadataProvider.listTables))

        case ShowColumns(tableName) => metadataProvider.describeTable(tableName).right map resultToProto

        case ShowQueryMetrics(filter, limit) =>
          Right(resultToProto(QueryInfoProvider.handleShowQueries(tsdb, filter, limit)))

        case KillQuery(filter) =>
          Right(resultToProto(QueryInfoProvider.handleKillQuery(tsdb, filter)))

        case DeleteQueryMetrics(filter) =>
          Right(resultToProto(QueryInfoProvider.handleDeleteQueryMetrics(tsdb, filter)))
      }
    }
  }

  def handleBatchQuery(tsdb: TSDB, batchSqlQuery: proto.BatchSqlQuery)(
      implicit ec: ExecutionContext
  ): Future[Either[String, Iterator[proto.Response]]] = {
    logger.debug(s"Processing batch SQL ${batchSqlQuery.sql} with ${batchSqlQuery.batch.size}")

    Future {
      SqlParser.parse(batchSqlQuery.sql).right.flatMap {
        case upsert: Upsert =>
          val params = batchSqlQuery.batch.map(ps => ps.parameters.map(p => p.index -> convertValue(p.value)).toMap)
          doUpsert(tsdb, upsert, params)

        case _ => Left(s"Only UPSERT can have batch parameters, but got ${batchSqlQuery.sql}")
      }
    }
  }

  def handlePingProto(
      tsdb: TSDB,
      ping: proto.Ping,
      majorVersion: Int,
      minorVersion: Int,
      version: String
  ): Either[String, Iterator[proto.Response]] = {
    logger.debug(s"Processing Ping request: $ping")

    if (ping.getVersion.protocol != ProtocolVersion.value) {
      logger.error(
        s"Incompatible protocols: driver protocol ${ping.getVersion.protocol}, server protocol ${ProtocolVersion.value}"
      )
      Left(
        s"Incompatible protocols: driver protocol ${ping.getVersion.protocol}, server protocol ${ProtocolVersion.value}"
      )
    } else {
      val pong = proto.Pong(
        ping.reqTime,
        System.currentTimeMillis(),
        Some(proto.Version(ProtocolVersion.value, majorVersion, minorVersion, version))
      )

      Right(Iterator(proto.Response(proto.Response.Resp.Pong(pong))))
    }
  }

  private def doUpsert(
      tsdb: TSDB,
      upsert: Upsert,
      params: Seq[Map[Int, Value]]
  ): Either[String, Iterator[proto.Response]] = {
    sqlQueryProcessor.createDataPoints(upsert, params).right.flatMap { dps =>
      tsdb.put(dps)
      Right(
        resultToProto(SimpleResult("RESULT", List("RESULT"), List(DataType[String]), Iterator(Array(Some("OK")))))
      )
    }
  }

  private def resultToProto(result: Result): Iterator[proto.Response] = {
    val rts = result.dataTypes.zipWithIndex
    val results = result.iterator.map { row =>
      val bytes = rts.map {
        case (rt, idx) =>
          val v = row.fieldByIndex[rt.T](idx)
          val b = v.map(rt.writable.write).getOrElse(Array.empty[Byte])
          ByteString.copyFrom(b)
      }
      proto.Response(proto.Response.Resp.Result(proto.ResultChunk(bytes)))
    }

    val resultFields = result.fieldNames.zip(result.dataTypes).map {
      case (name, rt) =>
        proto.ResultField(name, rt.meta.sqlTypeName)
    }
    val header = proto.Response(proto.Response.Resp.ResultHeader(proto.ResultHeader(resultFields, Some(result.name))))
    logger.debug("Response header: " + header)
    val footer = proto.Response(proto.Response.Resp.ResultStatistics(proto.ResultStatistics(-1, -1)))
    Iterator(header) ++ results ++ Iterator(footer)
  }

  private def convertValue(value: proto.Value): Value = {
    value.value match {
      case proto.Value.Value.TextValue(s)    => StringValue(s)
      case proto.Value.Value.DecimalValue(n) => NumericValue(BigDecimal(n))
      case proto.Value.Value.TimeValue(t)    => TimestampValue(t)
      case proto.Value.Value.Empty           => throw new IllegalArgumentException("Empty value")
    }
  }
}
