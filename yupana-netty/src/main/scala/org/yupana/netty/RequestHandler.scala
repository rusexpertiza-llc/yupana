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
import org.yupana.api.query.Result
import org.yupana.core.QueryEngineRouter
import org.yupana.core.sql.parser._
import org.yupana.protocol
import org.yupana.protocol.{ PrepareQuery, Response, ResultField, ResultFooter, ResultHeader, ResultRow }

import scala.concurrent.{ ExecutionContext, Future }

class RequestHandler(queryEngineRouter: QueryEngineRouter) extends StrictLogging {

  def handleQuery(sqlQuery: PrepareQuery)(
      implicit ec: ExecutionContext
  ): Future[Either[String, Iterator[Response[_]]]] = {
    logger.debug(s"""Processing SQL query: "${sqlQuery.query}"; parameters: ${sqlQuery.params}""")

    Future {
      val params = sqlQuery.params.map { case (index, p) => index -> convertValue(p) }
      queryEngineRouter.query(sqlQuery.query, params).map(resultToProto)
    }
  }

  private def resultToProto(result: Result): Iterator[Response[_]] = {
    val rts = result.dataTypes.zipWithIndex
    val results = result.map { row =>
      val bytes = rts.map {
        case (rt, idx) =>
          val v = row.get[rt.T](idx)
          if (v != null) rt.storable.write(v) else Array.empty[Byte]
      }
      ResultRow(bytes)
    }

    val resultFields = result.fieldNames.zip(result.dataTypes).map {
      case (name, rt) => ResultField(name, rt.meta.sqlTypeName)
    }
    val header = ResultHeader(result.name, resultFields)
    logger.debug("Response header: " + header)
    val footer = ResultFooter(-1, -1)
    Iterator(header) ++ results ++ Iterator(footer)
  }

  private def convertValue(value: protocol.ParameterValue): Value = {
    value match {
      case protocol.StringValue(s)    => StringValue(s)
      case protocol.NumericValue(n)   => NumericValue(n)
      case protocol.TimestampValue(t) => TimestampValue(t)
    }
  }
}
