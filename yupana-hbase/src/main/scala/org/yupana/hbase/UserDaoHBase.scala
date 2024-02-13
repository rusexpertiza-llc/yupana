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

package org.yupana.hbase

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.yupana.core.auth.{ TsdbRole, YupanaUser }
import org.yupana.core.dao.UserDao

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.Using

class UserDaoHBase(connection: Connection, namespace: String) extends UserDao {

  override def createUser(userName: String, password: Option[String], role: TsdbRole): Boolean = {
    withTable { table =>
      findUser(table, userName) match {
        case None =>
          writeUser(table, userName, password, role)
          true
        case Some(_) => false
      }
    }
  }

  override def updateUser(userName: String, password: Option[String], role: Option[TsdbRole]): Boolean = {
    withTable { table =>
      findUser(table, userName) match {
        case Some(user) =>
          val p = password orElse user.password
          val r = role getOrElse user.role
          writeUser(table, userName, p, r)
          true
        case None => false
      }
    }
  }

  override def deleteUser(userName: String): Boolean = {
    val delete = new Delete(Bytes.toBytes(userName))
    withTable { table =>
      if (findUser(table, userName).nonEmpty) {
        table.delete(delete)
        true
      } else false
    }
  }

  override def findUser(userName: String): Option[YupanaUser] = {
    withTable { table => findUser(table, userName) }
  }

  override def listUsers(): List[YupanaUser] = {
    withTable { table =>
      Using.resource(table.getScanner(UserDaoHBase.FAMILY)) { scanner =>
        scanner.asScala.map(extractUser).toList
      }
    }
  }

  private def getTable: Table = {
    connection.getTable(TableName.valueOf(namespace, UserDaoHBase.TABLE_NAME))
  }

  private def extractUser(r: Result): YupanaUser = {
    val name = Bytes.toString(r.getRow)
    val pass = Option(r.getValue(UserDaoHBase.FAMILY, UserDaoHBase.PASSWORD_QUALIFIER)).map(Bytes.toString)
    val roleStr = Bytes.toString(r.getValue(UserDaoHBase.FAMILY, UserDaoHBase.ROLE_QUALIFIER))
    val role = TsdbRole.roleByName(roleStr).getOrElse(throw new IllegalStateException(s"Invalid role $roleStr"))
    YupanaUser(name, pass, role)
  }

  private def findUser(table: Table, userName: String): Option[YupanaUser] = {
    val r = table.get(new Get(Bytes.toBytes(userName)))
    Option.when(!r.isEmpty)(extractUser(r))
  }

  private def writeUser(table: Table, name: String, password: Option[String], role: TsdbRole): Unit = {
    val put = new Put(Bytes.toBytes(name))
    put.addColumn(UserDaoHBase.FAMILY, UserDaoHBase.ROLE_QUALIFIER, Bytes.toBytes(role.name))
    password.foreach(p => put.addColumn(UserDaoHBase.FAMILY, UserDaoHBase.PASSWORD_QUALIFIER, Bytes.toBytes(p)))
    table.put(put)
  }

  private def withTable[T](block: Table => T): T = {
    HBaseUtils.checkTableExistsElseCreate(
      connection,
      TableName.valueOf(namespace, UserDaoHBase.TABLE_NAME),
      Seq(UserDaoHBase.FAMILY)
    )
    Using.resource(getTable)(block)
  }
}

object UserDaoHBase {
  val TABLE_NAME = "ts_users"
  val FAMILY: Array[Byte] = Bytes.toBytes("f")
  val ROLE_QUALIFIER: Array[Byte] = Bytes.toBytes("role")
  val PASSWORD_QUALIFIER: Array[Byte] = Bytes.toBytes("password")
}
