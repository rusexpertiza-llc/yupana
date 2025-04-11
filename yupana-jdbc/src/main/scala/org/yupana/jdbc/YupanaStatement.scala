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

class YupanaStatement(val connection: YupanaConnection) extends Statement {
  private var maxRows = 0
  private var fetchSize = 0
  private var closed = false
  protected var lastResultSet: YupanaResultSet = _

  @throws[SQLException]
  override def executeQuery(sql: String): ResultSet = {
    execute(sql)
    lastResultSet
  }

  @throws[SQLException]
  override def close(): Unit = {
    if (lastResultSet != null) {
      lastResultSet.close()
      lastResultSet = null
    }
    closed = true
  }

  @throws[SQLException]
  override def isClosed: Boolean = closed

  protected def checkClosed(): Unit = {
    if (isClosed) throw new YupanaException("Statement is already closed")
  }

  @throws[SQLException]
  override def execute(sql: String): Boolean = {
    checkClosed()
    val result = connection.runQuery(sql, Map.empty)
    lastResultSet = new YupanaResultSet(this, result.result, Some(result.id))
    true
  }

  @throws[SQLException]
  override def executeUpdate(s: String): Int =
    throw new SQLFeatureNotSupportedException("Method not supported: Statement.executeUpdate(String,int)")

  @throws[SQLException]
  override def getMaxFieldSize: Int =
    throw new SQLFeatureNotSupportedException("Method not supported: getMaxFieldSize()")

  @throws[SQLException]
  override def setMaxFieldSize(i: Int): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: setMaxFieldSize(int)")

  @throws[SQLException]
  override def getMaxRows: Int = maxRows

  @throws[SQLException]
  override def setMaxRows(maxRows: Int): Unit = this.maxRows = maxRows

  @throws[SQLException]
  override def setEscapeProcessing(b: Boolean): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: setEscapeProcessing(boolean)")

  @throws[SQLException]
  override def getQueryTimeout: Int = 0

  @throws[SQLException]
  override def setQueryTimeout(i: Int): Unit =
    if (i != 0) throw new SQLFeatureNotSupportedException("Timeout limit is not supported")

  @throws[SQLException]
  override def cancel(): Unit = {
    close()
  }

  @throws[SQLException]
  override def getWarnings: SQLWarning = null

  @throws[SQLException]
  override def clearWarnings(): Unit = {}

  @throws[SQLException]
  override def setCursorName(s: String): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: setCursorName(String)")

  @throws[SQLException]
  override def getResultSet: ResultSet = {
    if (closed) throw new SQLException("This statement is already closed")
    lastResultSet
  }

  @throws[SQLException]
  override def getUpdateCount: Int = -1

  @throws[SQLException]
  override def getMoreResults: Boolean = false

  @throws[SQLException]
  override def setFetchDirection(fetchDirection: Int): Unit = {
    if (fetchDirection != ResultSet.FETCH_FORWARD)
      throw new SQLException(s"Unsupported fetch direction $fetchDirection")
  }

  @throws[SQLException]
  override def getFetchDirection: Int = ResultSet.FETCH_FORWARD

  @throws[SQLException]
  override def setFetchSize(fetchSize: Int): Unit = this.fetchSize = fetchSize

  @throws[SQLException]
  override def getFetchSize: Int = fetchSize

  @throws[SQLException]
  override def getResultSetConcurrency: Int = ResultSet.CONCUR_READ_ONLY

  @throws[SQLException]
  override def getResultSetType: Int = ResultSet.TYPE_FORWARD_ONLY

  @throws[SQLException]
  override def addBatch(s: String): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: Statement.addBatch(String)")

  @throws[SQLException]
  override def clearBatch(): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: Statement.clearBatch()")

  @throws[SQLException]
  override def executeBatch: Array[Int] =
    throw new SQLFeatureNotSupportedException("Method not supported: Statement.executeBatch()")

  @throws[SQLException]
  override def getConnection: Connection = this.connection

  @throws[SQLException]
  override def getMoreResults(current: Int): Boolean =
    throw new SQLFeatureNotSupportedException("Method not supported: Statement.getMoreResults(int)")

  @throws[SQLException]
  override def getGeneratedKeys: ResultSet =
    throw new SQLFeatureNotSupportedException("Method not supported: Statement.getGeneratedKeys()")

  @throws[SQLException]
  override def executeUpdate(sql: String, autoGeneratedKeys: Int): Int =
    throw new SQLFeatureNotSupportedException("Method not supported: Statement.executeUpdate(String,int)")

  @throws[SQLException]
  override def executeUpdate(sql: String, columnIndexes: Array[Int]): Int =
    throw new SQLFeatureNotSupportedException("Method not supported: : Statement.executeUpdate(String,int[])")

  @throws[SQLException]
  override def executeUpdate(sql: String, columnNames: Array[String]): Int =
    throw new SQLFeatureNotSupportedException("Method not supported: Statement.executeUpdate(String,String[])")

  @throws[SQLException]
  override def execute(sql: String, autoGeneratedKeys: Int): Boolean =
    throw new SQLFeatureNotSupportedException("Method not supported: Statement.execute(String,int)")

  @throws[SQLException]
  override def execute(sql: String, columnIndexes: Array[Int]): Boolean =
    throw new SQLFeatureNotSupportedException("Method not supported: Statement.execute(String,int[])")

  @throws[SQLException]
  override def execute(sql: String, columnNames: Array[String]): Boolean =
    throw new SQLFeatureNotSupportedException("Method not supported: Statement.execute(String,String[])")

  @throws[SQLException]
  override def getResultSetHoldability: Int = ResultSet.CLOSE_CURSORS_AT_COMMIT

  @throws[SQLException]
  override def setPoolable(b: Boolean): Unit = throw new SQLFeatureNotSupportedException("Pooling is not supported")

  @throws[SQLException]
  override def isPoolable: Boolean = false

  @throws[SQLException]
  override def closeOnCompletion(): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: Statement.closeOnCompletion()")

  @throws[SQLException]
  override def isCloseOnCompletion: Boolean = false

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
