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
import org.yupana.core.auth.{ TsdbRole, YupanaUser }
import org.yupana.core.dao.UserDao
import org.yupana.core.providers.{ JdbcMetadataProvider, QueryInfoProvider, UpdatesIntervalsProvider }
import org.yupana.core.sql.SqlQueryProcessor
import org.yupana.core.sql.parser._

class QueryEngineRouter(
    timeSeriesQueryEngine: TimeSeriesQueryEngine,
    flatQueryEngine: FlatQueryEngine,
    metadataProvider: JdbcMetadataProvider,
    sqlQueryProcessor: SqlQueryProcessor,
    userDao: UserDao
) {

  def query(user: YupanaUser, sql: String, params: Map[Int, Value]): Either[String, Result] = {
    SqlParser.parse(sql) flatMap {
      case select: Select =>
        val tsdbQuery: Either[String, Query] = sqlQueryProcessor.createQuery(select, params)
        tsdbQuery flatMap { query =>
          Right(timeSeriesQueryEngine.query(user, query))
        }

      case upsert: Upsert =>
        doUpsert(user, upsert, Seq(params))

      case ShowTables => Right(metadataProvider.listTables)

      case ShowVersion => Right(metadataProvider.version)

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

      case CreateUser(u, p, r) => createUser(user, u, p, r)
      case DropUser(u)         => deleteUser(user, u)
      case AlterUser(u, p, r)  => updateUser(user, u, p, r)
      case ShowUsers           => listUsers(user)
    }
  }

  def singleResult[T](name: String, value: T)(implicit dt: DataType.Aux[T]): Result = {
    SimpleResult("RESULT", List(name), List(dt), Iterator(Array[Any](value)))
  }
  def createUser(
      user: YupanaUser,
      name: String,
      password: Option[String],
      role: Option[String]
  ): Either[String, Result] = {
    for {
      _ <- isAdmin(user)
      r <- role.map(r => getRole(r)).getOrElse(Right(TsdbRole.Disabled))
    } yield {
      userDao.createUser(name, password, r)
      singleResult("STATUS", "OK")
    }
  }

  def deleteUser(user: YupanaUser, name: String): Either[String, Result] = {
    isAdmin(user).flatMap(_ =>
      if (userDao.deleteUser(name)) Right(singleResult("STATUS", "OK")) else Left("User not found")
    )
  }

  def updateUser(
      user: YupanaUser,
      name: String,
      password: Option[String],
      role: Option[String]
  ): Either[String, Result] = {
    for {
      _ <- isAdmin(user)
      r <- role.fold(Right(None): Either[String, Option[TsdbRole]])(x => getRole(x).map(Some(_)))
    } yield {
      userDao.updateUser(name, password, r)
      singleResult("STATUS", "OK")
    }
  }

  def listUsers(user: YupanaUser): Either[String, Result] = {
    isAdmin(user).map { _ =>
      val users = userDao.listUsers()
      SimpleResult(
        "USERS",
        List("NAME", "ROLE"),
        List(DataType[String], DataType[String]),
        users.map(u => Array[Any](u.name, u.role.name)).iterator
      )
    }
  }

  def getRole(name: String): Either[String, TsdbRole] = {
    TsdbRole.roleByName(name).toRight(s"Invalid role name '$name'")
  }

  def isAdmin(user: YupanaUser): Either[String, YupanaUser] = {
    if (user.role == TsdbRole.Admin) Right(user) else Left(s"User ${user.name} doesn't have enough permissions")
  }

  def batchQuery(user: YupanaUser, sql: String, params: Seq[Map[Int, Value]]): Either[String, Result] = {
    SqlParser.parse(sql).flatMap {
      case upsert: Upsert =>
        doUpsert(user, upsert, params)
      case _ => Left(s"Only UPSERT can have batch parameters, but got ${sql}")
    }
  }

  private def doUpsert(
      user: YupanaUser,
      upsert: Upsert,
      params: Seq[Map[Int, Value]]
  ): Either[String, Result] = {
    sqlQueryProcessor.createDataPoints(upsert, params).flatMap { dps =>
      timeSeriesQueryEngine.put(user, dps)
      Right(singleResult("RESULT", "OK"))
    }
  }

}
