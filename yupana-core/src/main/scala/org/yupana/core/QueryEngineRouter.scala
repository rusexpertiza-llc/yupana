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

package org.yupana.core

import org.yupana.api.query.{ Query, Result, SimpleResult }
import org.yupana.api.types.DataType
import org.yupana.core.providers.{ UpdatesIntervalsProvider, JdbcMetadataProvider, QueryInfoProvider }
import org.yupana.core.sql.SqlQueryProcessor
import org.yupana.core.sql.parser._

class QueryEngineRouter(
    timeSeriesQueryEngine: TimeSeriesQueryEngine,
    flatQueryEngine: FlatQueryEngine,
    metadataProvider: JdbcMetadataProvider,
    sqlQueryProcessor: SqlQueryProcessor
) {

  def query(sql: String, params: Map[Int, Value]): Either[String, Result] = {
    SqlParser.parse(sql) flatMap {
      case select: Select =>
        val tsdbQuery: Either[String, Query] = sqlQueryProcessor.createQuery(select, params)
        tsdbQuery flatMap { query =>
          Right(timeSeriesQueryEngine.query(query))
        }

      case upsert: Upsert =>
        doUpsert(upsert, Seq(params))

      case ShowTables => Right(metadataProvider.listTables)

      case ShowColumns(tableName) => metadataProvider.describeTable(tableName)

      case ShowFunctions(typeName) => metadataProvider.listFunctions(typeName)

      case ShowQueryMetrics(filter, limit) =>
        Right(QueryInfoProvider.handleShowQueries(flatQueryEngine, filter, limit))

      case KillQuery(filter) =>
        Right(QueryInfoProvider.handleKillQuery(flatQueryEngine, filter))

      case DeleteQueryMetrics(filter) =>
        Right(QueryInfoProvider.handleDeleteQueryMetrics(flatQueryEngine, filter))

      case ShowUpdatesIntervals(condition) =>
        UpdatesIntervalsProvider.handleGetUpdatesIntervals(flatQueryEngine, condition, params)
    }
  }

  def batchQuery(sql: String, params: Seq[Map[Int, Value]]): Either[String, Result] = {
    SqlParser.parse(sql).flatMap {
      case upsert: Upsert =>
        doUpsert(upsert, params)
      case _ => Left(s"Only UPSERT can have batch parameters, but got ${sql}")
    }
  }

  private def doUpsert(
      upsert: Upsert,
      params: Seq[Map[Int, Value]]
  ): Either[String, Result] = {
    sqlQueryProcessor.createDataPoints(upsert, params).flatMap { dps =>
      timeSeriesQueryEngine.put(dps)
      Right(
        SimpleResult("RESULT", List("RESULT"), List(DataType[String]), Iterator(Array("OK")))
      )
    }
  }

}
