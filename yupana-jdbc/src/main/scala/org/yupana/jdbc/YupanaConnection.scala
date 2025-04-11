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

package org.yupana.jdbc

import java.sql.{ Array => _, _ }
import org.yupana.api.query.Result
import org.yupana.jdbc.YupanaConnection.QueryResult
import org.yupana.protocol.ParameterValue

import java.util
import java.util.Properties
import java.util.concurrent.Executor
import scala.util.Using

trait YupanaConnection extends Connection {

  // By default JDBC connection is in auto-commit mode
  private var autoCommit = true

  def runQuery(query: String, params: Map[Int, ParameterValue]): QueryResult
  def runBatchQuery(query: String, params: Seq[Map[Int, ParameterValue]]): QueryResult
  def url: String

  def cancelStream(streamId: Int): Unit

  @throws[SQLException]
  override def createStatement: Statement = {
    new YupanaStatement(this)
  }

  @throws[SQLException]
  override def prepareStatement(sql: String): PreparedStatement = {
    new YupanaPreparedStatement(this, sql)
  }

  @throws[SQLException]
  override def prepareCall(s: String): CallableStatement = {
    throw new SQLFeatureNotSupportedException("Method not supported: Connection.prepareCall(String)")
  }

  @throws[SQLException]
  override def nativeSQL(s: String): String = {
    throw new SQLFeatureNotSupportedException("Method not supported: Connection.nativeSQL(String)")
  }

  @throws[SQLException]
  override def setAutoCommit(autoCommit: Boolean): Unit = {
    this.autoCommit = autoCommit
  }

  @throws[SQLException]
  override def getAutoCommit: Boolean = autoCommit

  @throws[SQLException]
  override def commit(): Unit = {}

  @throws[SQLException]
  override def rollback(): Unit = {}

  @throws[SQLException]
  override lazy val getMetaData: DatabaseMetaData = new YupanaDatabaseMetaData(this)

  @throws[SQLException]
  override def setReadOnly(b: Boolean): Unit = {}

  @throws[SQLException]
  override def isReadOnly = true

  @throws[SQLException]
  override def setCatalog(s: String): Unit = {}

  @throws[SQLException]
  override def getCatalog: String = null

  @throws[SQLException]
  override def setTransactionIsolation(level: Int): Unit = {
    if (level != Connection.TRANSACTION_NONE)
      throw new SQLFeatureNotSupportedException(s"Unsupported transaction isolation level $level")
  }

  @throws[SQLException]
  override def getTransactionIsolation: Int = Connection.TRANSACTION_NONE

  @throws[SQLException]
  override def getWarnings: SQLWarning = null

  @throws[SQLException]
  override def clearWarnings(): Unit = {}

  @throws[SQLException]
  override def createStatement(resultSetType: Int, resultSetConcurrency: Int): Statement = {
    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
      throw new SQLFeatureNotSupportedException(
        s"Unsupported statement type $resultSetType or concurrency: $resultSetConcurrency"
      )
    }

    createStatement()
  }

  @throws[SQLException]
  override def prepareStatement(sql: String, resultSetType: Int, resultSetConcurrency: Int): PreparedStatement = {
    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
      throw new SQLFeatureNotSupportedException(
        s"Unsupported prepared statement type $resultSetType or concurrency: $resultSetConcurrency"
      )
    }

    prepareStatement(sql)
  }

  @throws[SQLException]
  override def prepareCall(s: String, i: Int, i1: Int): CallableStatement = {
    throw new SQLFeatureNotSupportedException("Method not supported: Connection.prepareCall(String, int, int)")
  }

  @throws[SQLException]
  override def getTypeMap: util.Map[String, Class[_]] = null

  @throws[SQLException]
  override def setTypeMap(map: util.Map[String, Class[_]]): Unit = {
    JdbcUtils.checkTypeMapping(map)
  }

  @throws[SQLException]
  override def setHoldability(holdability: Int): Unit =
    if (holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT)
      throw new SQLFeatureNotSupportedException("Unsupported holdability: " + holdability)

  @throws[SQLException]
  override def getHoldability: Int = ResultSet.HOLD_CURSORS_OVER_COMMIT

  @throws[SQLException]
  override def setSavepoint(): Savepoint = {
    throw new SQLFeatureNotSupportedException("Method not supported: Connection.setSavepoint()")
  }

  @throws[SQLException]
  override def setSavepoint(s: String): Savepoint = {
    throw new SQLFeatureNotSupportedException("Method not supported: Connection.setSavepoint(s)")
  }

  @throws[SQLException]
  override def rollback(savepoint: Savepoint): Unit = {
    throw new SQLFeatureNotSupportedException("MethodNotSupported: Connection.rollback(Savepoint)")
  }

  @throws[SQLException]
  override def releaseSavepoint(savepoint: Savepoint): Unit = {
    throw new SQLFeatureNotSupportedException("Method not supported: Connection.releaseSavepoint(Savepoint)")
  }

  @throws[SQLException]
  override def createStatement(resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): Statement = {
    if (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
      throw new SQLFeatureNotSupportedException(s"Unsupported statement holdability $resultSetConcurrency")
    }

    createStatement(resultSetType, resultSetConcurrency)
  }

  @throws[SQLException]
  override def prepareStatement(
      sql: String,
      resultSetType: Int,
      resultSetConcurrency: Int,
      resultSetHoldability: Int
  ): PreparedStatement = {
    if (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
      throw new SQLFeatureNotSupportedException(s"Unsupported statement holdability $resultSetConcurrency")
    }

    prepareStatement(sql, resultSetType, resultSetConcurrency)
  }

  @throws[SQLException]
  override def prepareCall(sql: String, resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int) =
    throw new UnsupportedOperationException("Method not supported: Connection.prepareCall(String,int,int,int)")

  @throws[SQLException]
  override def prepareStatement(sql: String, autoGeneratedKeys: Int): PreparedStatement = {
    throw new SQLFeatureNotSupportedException("Method not supported: Connection.prepareStatement(String,int)")
  }

  @throws[SQLException]
  override def prepareStatement(sql: String, columnIndexes: Array[Int]): PreparedStatement = {
    throw new SQLFeatureNotSupportedException("Method not supported: Connection.prepareStatement(String,int[])")
  }

  @throws[SQLException]
  override def prepareStatement(sql: String, columnNames: Array[String]): PreparedStatement = {
    throw new SQLFeatureNotSupportedException("Method not supported: Connection.prepareStatement(String,String[])")
  }

  @throws[SQLException]
  override def createClob: Clob = throw new SQLFeatureNotSupportedException("CLOBs are not supported")

  @throws[SQLException]
  override def createBlob: Blob = throw new SQLFeatureNotSupportedException("BLOBs are not supported")

  @throws[SQLException]
  override def createNClob: NClob = throw new SQLFeatureNotSupportedException("NCLOBs are not supported")

  @throws[SQLException]
  override def createSQLXML: SQLXML = throw new SQLFeatureNotSupportedException("SQLXMLs are not supported")

  @throws[SQLException]
  // NOTE: we do not support timeouts (yet?)
  override def isValid(i: Int): Boolean = {
    if (!isClosed) {
      Using.resource(createStatement()) { statement =>
        Using.resource(statement.executeQuery("SELECT 1")) { rs =>
          rs.next() && rs.getBigDecimal(1) == java.math.BigDecimal.ONE
        }
      }
    } else false
  }

  @throws[SQLClientInfoException]
  override def setClientInfo(s: String, s1: String): Unit = {
    throw new SQLClientInfoException("Method is not supported", null)
  }

  @throws[SQLClientInfoException]
  override def setClientInfo(properties: Properties): Unit = {
    throw new SQLClientInfoException("Method is not supported", null)
  }

  @throws[SQLException]
  override def getClientInfo(s: String): String = throw new SQLClientInfoException("Client info is not supported", null)

  @throws[SQLException]
  override def getClientInfo: Properties = throw new SQLClientInfoException("Client info is not supported", null)

  @throws[SQLException]
  override def createArrayOf(s: String, objects: Array[AnyRef]): java.sql.Array =
    throw new SQLFeatureNotSupportedException("Arrays are not supported")

  @throws[SQLException]
  override def createStruct(s: String, objects: Array[AnyRef]): Struct =
    throw new SQLFeatureNotSupportedException("Structs are not supported")

  @throws[SQLException]
  override def setSchema(s: String): Unit = {}

  @throws[SQLException]
  override def getSchema: String = null

  @throws[SQLException]
  override def abort(executor: Executor): Unit = {
    throw new SQLFeatureNotSupportedException("Method not found: Connection.abort(Executor)")
  }

  @throws[SQLException]
  override def setNetworkTimeout(executor: Executor, i: Int): Unit = {
    throw new SQLFeatureNotSupportedException("Method is not supported")
  }

  @throws[SQLException]
  override def getNetworkTimeout: Int = throw new SQLFeatureNotSupportedException("Method is not supported")

  @throws[SQLException]
  override def unwrap[T](aClass: Class[T]): T = {
    if (!aClass.isAssignableFrom(getClass)) {
      throw new SQLException(s"Cannot unwrap to ${aClass.getName}")
    }

    aClass.cast(this)
  }

  @throws[SQLException]
  override def isWrapperFor(aClass: Class[_]): Boolean = aClass.isAssignableFrom(getClass)
}

object YupanaConnection {
  case class QueryResult(id: Int, result: Result)
}
