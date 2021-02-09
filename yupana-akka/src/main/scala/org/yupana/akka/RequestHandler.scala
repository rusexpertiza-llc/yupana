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
import org.yupana.api.query.Result
import org.yupana.core.QueryEngineRouter
import org.yupana.core.sql.parser._
import org.yupana.proto
import org.yupana.proto.util.ProtocolVersion

import scala.concurrent.{ ExecutionContext, Future }

class RequestHandler(queryEngineRouter: QueryEngineRouter) extends StrictLogging {

  def handleQuery(sqlQuery: proto.SqlQuery)(
      implicit ec: ExecutionContext
  ): Future[Either[String, Iterator[proto.Response]]] = {
    logger.debug(s"""Processing SQL query: "${sqlQuery.sql}"; parameters: ${sqlQuery.parameters}""")

    Future {
      val params = sqlQuery.parameters.map(p => p.index -> convertValue(p.value)).toMap
      queryEngineRouter.query(sqlQuery.sql, params).map(resultToProto(_))
    }
  }

  def handleBatchQuery(batchSqlQuery: proto.BatchSqlQuery)(
      implicit ec: ExecutionContext
  ): Future[Either[String, Iterator[proto.Response]]] = {
    logger.debug(s"Processing batch SQL ${batchSqlQuery.sql} with ${batchSqlQuery.batch.size}")

    Future {
      val params = batchSqlQuery.batch.map(ps => ps.parameters.map(p => p.index -> convertValue(p.value)).toMap)
      queryEngineRouter.batchQuery(batchSqlQuery.sql, params).map(resultToProto(_))
    }
  }

  def handlePingProto(
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

  private def resultToProto(result: Result): Iterator[proto.Response] = {
    val rts = result.dataTypes.zipWithIndex
    val results = result.iterator.map { row =>
      val bytes = rts.map {
        case (rt, idx) =>
          val v = row.get[rt.T](idx)
          val b = if (v != null) rt.storable.write(v) else Array.empty[Byte]
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
