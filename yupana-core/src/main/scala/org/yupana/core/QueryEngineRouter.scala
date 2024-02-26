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

import org.yupana.api.query.{ Result, SimpleResult }
import org.yupana.api.types.DataType
import org.yupana.core.auth.{ Action, PermissionService, Subject, UserManager, YupanaUser }
import org.yupana.core.providers.{ JdbcMetadataProvider, QueryInfoProvider, UpdatesIntervalsProvider }
import org.yupana.core.sql.SqlQueryProcessor
import org.yupana.core.sql.parser._

class QueryEngineRouter(
    timeSeriesQueryEngine: TSDB,
    flatQueryEngine: FlatQueryEngine,
    metadataProvider: JdbcMetadataProvider,
    sqlQueryProcessor: SqlQueryProcessor,
    permissionService: PermissionService,
    userManager: UserManager
) {

  def query(user: YupanaUser, sql: String, params: Map[Int, Value]): Either[String, Result] = {
    SqlParser.parse(sql) flatMap {
      case select: Select =>
        for {
          _ <- hasPermission(user, Subject.Table(select.tableName), Action.Read)
          query <- sqlQueryProcessor.createQuery(select, params)
        } yield timeSeriesQueryEngine.query(query, user)

      case upsert: Upsert =>
        hasPermission(user, Subject.Table(Some(upsert.tableName)), Action.Write)
          .flatMap(_ => doUpsert(user, upsert, Seq(params)))

      case ShowTables => hasPermission(user, Subject.Metadata, Action.Read).map(_ => metadataProvider.listTables)

      case ShowVersion => hasPermission(user, Subject.Metadata, Action.Read).map(_ => metadataProvider.version)

      case ShowColumns(tableName) =>
        hasPermission(user, Subject.Metadata, Action.Read).flatMap(_ => metadataProvider.describeTable(tableName))

      case ShowFunctions(typeName) =>
        hasPermission(user, Subject.Metadata, Action.Read).flatMap(_ => metadataProvider.listFunctions(typeName))

      case ShowQueryMetrics(filter, limit) =>
        hasPermission(user, Subject.Queries, Action.Read).map(_ =>
          QueryInfoProvider.handleShowQueries(flatQueryEngine, filter, limit)
        )

      case KillQuery(filter) =>
        hasPermission(user, Subject.Queries, Action.Write).map(_ =>
          QueryInfoProvider.handleKillQuery(flatQueryEngine, filter)
        )

      case DeleteQueryMetrics(filter) =>
        hasPermission(user, Subject.Queries, Action.Write).map(_ =>
          QueryInfoProvider.handleDeleteQueryMetrics(flatQueryEngine, filter)
        )

      case ShowUpdatesIntervals(condition) =>
        hasPermission(user, Subject.Queries, Action.Read).flatMap(_ =>
          UpdatesIntervalsProvider.handleGetUpdatesIntervals(flatQueryEngine, condition, params)
        )

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
      _ <- hasPermission(user, Subject.User, Action.Write)
      _ <- userManager.createUser(name, password, role)
    } yield singleResult("STATUS", "OK")
  }

  def deleteUser(user: YupanaUser, name: String): Either[String, Result] = {
    hasPermission(user, Subject.User, Action.Write).flatMap(_ =>
      if (userManager.deleteUser(name)) Right(singleResult("STATUS", "OK")) else Left("User not found")
    )
  }

  def updateUser(
      user: YupanaUser,
      name: String,
      password: Option[String],
      role: Option[String]
  ): Either[String, Result] = {
    for {
      _ <- hasPermission(user, Subject.User, Action.Write)
      _ <- userManager.updateUser(name, password, role)
    } yield singleResult("STATUS", "OK")
  }

  def listUsers(user: YupanaUser): Either[String, Result] = {
    hasPermission(user, Subject.User, Action.Read).map { _ =>
      val users = userManager.listUsers()
      SimpleResult(
        "USERS",
        List("NAME", "ROLE"),
        List(DataType[String], DataType[String]),
        users.map(u => Array[Any](u.name, u.role.name)).iterator
      )
    }
  }

  def hasPermission(user: YupanaUser, subject: Subject, action: Action): Either[String, YupanaUser] = {
    if (permissionService.hasPermission(user, subject, action)) Right(user)
    else Left(s"User ${user.name} doesn't have enough permissions")
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
      timeSeriesQueryEngine.put(dps.iterator, user)
      Right(singleResult("RESULT", "OK"))
    }
  }

}
