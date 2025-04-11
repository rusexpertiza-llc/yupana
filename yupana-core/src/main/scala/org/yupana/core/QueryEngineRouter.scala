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
import org.yupana.api.types.{ DataType, StringReaderWriter }
import org.yupana.core.auth.{ Action, Object, PermissionService, UserManager, YupanaUser }
import org.yupana.core.providers.{ JdbcMetadataProvider, QueryInfoProvider, UpdatesIntervalsProvider }
import org.yupana.core.sql.{ Parameter, SqlQueryProcessor }
import org.yupana.core.sql.parser._

class QueryEngineRouter(
    tsdb: TSDB,
    flatQueryEngine: FlatQueryEngine,
    metadataProvider: JdbcMetadataProvider,
    sqlQueryProcessor: SqlQueryProcessor,
    permissionService: PermissionService,
    userManager: UserManager
) {

  def query(user: YupanaUser, sql: String, params: Map[Int, Parameter])(
      implicit srw: StringReaderWriter
  ): Either[String, Result] = {
    for {
      parsed <- parse(sql)
      prepared <- bind(parsed, params)
      result <- execute(user, prepared)
    } yield result
  }

  def parse(sql: String): Either[String, Statement] = {
    SqlParser.parse(sql)
  }

  def bind(statement: Statement, params: Map[Int, Parameter])(
      implicit srw: StringReaderWriter
  ): Either[String, PreparedStatement] = {
    statement match {
      case select: Select =>
        sqlQueryProcessor
          .createQuery(select)
          .flatMap(q => sqlQueryProcessor.bindParameters(q, params).map(PreparedSelect(q, _)))
      case ShowTables =>
        val meta = metadataProvider.listTablesMeta
        Right(PreparedCommand(ShowTables, params, meta._1, meta._2))
      case x => Right(PreparedCommand(x, params, Nil, Nil))
    }
  }

  def execute(user: YupanaUser, statement: PreparedStatement)(
      implicit srw: StringReaderWriter
  ): Either[String, Result] = {
    statement match {
      case select: PreparedSelect =>
        for {
          _ <- hasPermission(user, Object.Table(select.query.table.map(_.name)), Action.Read)
        } yield tsdb.query(select.query, params = select.params, user = user)

      case EmptyQuery => Right(SimpleResult("", Nil, Nil, Iterator.empty))

      case PreparedCommand(statement, params, _, _) =>
        statement match {
          case upsert: Upsert =>
            hasPermission(user, Object.Table(Some(upsert.tableName)), Action.Write)
              .flatMap(_ => doUpsert(user, upsert, Seq(params)))

          case ShowTables => hasPermission(user, Object.Metadata, Action.Read).map(_ => metadataProvider.listTables)

          case ShowVersion => hasPermission(user, Object.Metadata, Action.Read).map(_ => metadataProvider.version)

          case ShowColumns(tableName) =>
            hasPermission(user, Object.Metadata, Action.Read).flatMap(_ => metadataProvider.describeTable(tableName))

          case ShowFunctions(typeName) =>
            hasPermission(user, Object.Metadata, Action.Read).flatMap(_ => metadataProvider.listFunctions(typeName))

          case ShowQueryMetrics(filter, limit) =>
            hasPermission(user, Object.Queries, Action.Read).map(_ =>
              QueryInfoProvider.handleShowQueries(flatQueryEngine, filter, limit)
            )

          case KillQuery(filter) =>
            hasPermission(user, Object.Queries, Action.Write).map(_ =>
              QueryInfoProvider.handleKillQuery(flatQueryEngine, filter)
            )

          case DeleteQueryMetrics(filter) =>
            hasPermission(user, Object.Queries, Action.Write).map(_ =>
              QueryInfoProvider.handleDeleteQueryMetrics(flatQueryEngine, filter)
            )

          case ShowUpdatesIntervals(condition) =>
            hasPermission(user, Object.Queries, Action.Read).flatMap(_ =>
              UpdatesIntervalsProvider.handleGetUpdatesIntervals(flatQueryEngine, condition, params)
            )

          case CreateUser(u, p, r) => createUser(user, u, p, r)
          case DropUser(u)         => deleteUser(user, u)
          case AlterUser(u, p, r)  => updateUser(user, u, p, r)
          case ShowUsers           => listUsers(user)
          case x                   => Left(s"Unsupported query type $x")
        }
    }
  }

  private def singleResult[T](name: String, value: T)(implicit dt: DataType.Aux[T]): Result = {
    SimpleResult("RESULT", List(name), List(dt), Iterator(Array[Any](value)))
  }

  private def createUser(
      user: YupanaUser,
      name: String,
      password: Option[String],
      role: Option[String]
  ): Either[String, Result] = {
    for {
      _ <- hasPermission(user, Object.User, Action.Write)
      _ <- userManager.createUser(name, password, role)
    } yield singleResult("STATUS", "OK")
  }

  private def deleteUser(user: YupanaUser, name: String): Either[String, Result] = {
    hasPermission(user, Object.User, Action.Write).flatMap(_ =>
      if (userManager.deleteUser(name)) Right(singleResult("STATUS", "OK")) else Left("User not found")
    )
  }

  private def updateUser(
      user: YupanaUser,
      name: String,
      password: Option[String],
      role: Option[String]
  ): Either[String, Result] = {
    for {
      _ <- hasPermission(user, Object.User, Action.Write)
      _ <- userManager.updateUser(name, password, role)
    } yield singleResult("STATUS", "OK")
  }

  private def listUsers(user: YupanaUser): Either[String, Result] = {
    hasPermission(user, Object.User, Action.Read).map { _ =>
      val users = userManager.listUsers()
      SimpleResult(
        "USERS",
        List("NAME", "ROLE"),
        List(DataType[String], DataType[String]),
        users.map(u => Array[Any](u.name, u.role.name)).iterator
      )
    }
  }

  private def hasPermission(user: YupanaUser, subject: Object, action: Action): Either[String, YupanaUser] = {
    if (permissionService.hasPermission(user, subject, action)) Right(user)
    else Left(s"User ${user.name} doesn't have enough permissions")
  }

  def batchQuery(user: YupanaUser, sql: String, params: Seq[Map[Int, Parameter]])(
      implicit srw: StringReaderWriter
  ): Either[String, Result] = {
    SqlParser.parse(sql).flatMap {
      case upsert: Upsert =>
        hasPermission(user, Object.Table(Some(upsert.tableName)), Action.Write)
          .flatMap(_ => doUpsert(user, upsert, params))
      case _ => Left(s"Only UPSERT can have batch parameters, but got $sql")
    }
  }

  private def doUpsert(
      user: YupanaUser,
      upsert: Upsert,
      params: Seq[Map[Int, Parameter]]
  )(implicit srw: StringReaderWriter): Either[String, Result] = {
    sqlQueryProcessor.createDataPoints(upsert, params).flatMap { dps =>
      tsdb.put(dps.iterator, user)
      Right(singleResult("RESULT", "OK"))
    }
  }

}
