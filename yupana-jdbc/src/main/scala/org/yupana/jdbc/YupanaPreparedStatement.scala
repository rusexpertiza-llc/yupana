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

import org.yupana.protocol.{ NumericValue, ParameterValue, StringValue, TimestampValue }

import java.io.{ InputStream, Reader }
import java.net.URL
import java.sql.{ Array => SqlArray, _ }
import java.util.Calendar
import java.util.logging.{ Level, Logger }
import scala.collection.mutable.ArrayBuffer

object YupanaPreparedStatement {
  private val LOGGER: Logger = Logger.getLogger(classOf[YupanaPreparedStatement].getName)
}

class YupanaPreparedStatement protected[jdbc] (connection: YupanaConnection, templateQuery: String)
    extends YupanaStatement(connection)
    with PreparedStatement {

  private var parameters = Map.empty[Int, ParameterValue]
  private val batch = ArrayBuffer.empty[Map[Int, ParameterValue]]

  private def setParameter(idx: Int, v: ParameterValue): Unit = {
    parameters += idx -> v
  }

  @throws[SQLException]
  override def addBatch(): Unit = {
    batch += parameters
    parameters = Map.empty
  }

  @throws[SQLException]
  override def clearParameters(): Unit = {
    parameters = Map.empty
  }

  @throws[SQLException]
  override def clearBatch(): Unit = {
    batch.clear()
  }

  @throws[SQLException]
  override def execute: Boolean = {
    YupanaPreparedStatement.LOGGER.log(Level.FINE, "Execute prepared statement {0}", templateQuery)
    val result = connection.runQuery(templateQuery, parameters)
    lastResultSet = new YupanaResultSet(this, result)
    true
  }

  @throws[SQLException]
  override def executeBatch: Array[Int] = {
    YupanaPreparedStatement.LOGGER.log(Level.FINE, "Execute prepared statement {0}", templateQuery)
    if (batch.isEmpty) throw new SQLException("Batch is not defined")
    val result = connection.runBatchQuery(templateQuery, batch.toSeq)
    lastResultSet = new YupanaResultSet(this, result)
    Array.fill(batch.size)(1)
  }

  @throws[SQLException]
  override def executeQuery: ResultSet = {
    execute
    lastResultSet
  }

  @throws[SQLException]
  override def executeUpdate: Int = 0

  @throws[SQLException]
  override def getMetaData: ResultSetMetaData = null

  @throws[SQLException]
  override def getParameterMetaData: ParameterMetaData = null

  @throws[SQLException]
  override def setArray(parameterIndex: Int, x: SqlArray): Unit = {
    throw new SQLFeatureNotSupportedException("SqlArrays are not supported")
  }

  @throws[SQLException]
  override def setAsciiStream(parameterIndex: Int, x: InputStream): Unit = {
    throw new SQLFeatureNotSupportedException("InputStreams are not supported")
  }

  @throws[SQLException]
  override def setAsciiStream(parameterIndex: Int, x: InputStream, length: Int): Unit = {
    throw new SQLFeatureNotSupportedException("InputStreams are not supported")
  }

  @throws[SQLException]
  override def setAsciiStream(parameterIndex: Int, x: InputStream, length: Long): Unit = {
    throw new SQLFeatureNotSupportedException("InputStreams are not supported")
  }

  @throws[SQLException]
  override def setBigDecimal(parameterIndex: Int, x: java.math.BigDecimal): Unit = {
    setParameter(parameterIndex, NumericValue(x))
  }

  @throws[SQLException]
  override def setBinaryStream(parameterIndex: Int, x: InputStream): Unit = {
    throw new SQLFeatureNotSupportedException("InputStreams are not supported")
  }

  @throws[SQLException]
  override def setBinaryStream(parameterIndex: Int, x: InputStream, length: Int): Unit = {
    throw new SQLFeatureNotSupportedException("InputStreams are not supported")
  }

  @throws[SQLException]
  override def setBinaryStream(parameterIndex: Int, x: InputStream, length: Long): Unit = {
    throw new SQLFeatureNotSupportedException("InputStreams are not supported")
  }

  @throws[SQLException]
  override def setBlob(parameterIndex: Int, x: Blob): Unit = {
    throw new SQLFeatureNotSupportedException("Blobs are not supported")
  }

  @throws[SQLException]
  override def setBlob(parameterIndex: Int, x: InputStream): Unit = {
    throw new SQLFeatureNotSupportedException("Blobs are not supported")
  }

  @throws[SQLException]
  override def setBlob(parameterIndex: Int, x: InputStream, length: Long): Unit = {
    throw new SQLFeatureNotSupportedException("Blobs are not supported")
  }

  @throws[SQLException]
  override def setBoolean(parameterIndex: Int, x: Boolean): Unit = {
    throw new SQLFeatureNotSupportedException("Booleans are not supported")
  }

  @throws[SQLException]
  override def setBytes(parameterIndex: Int, x: Array[Byte]): Unit = {
    throw new SQLFeatureNotSupportedException("Bytes are not supported")
  }

  @throws[SQLException]
  override def setCharacterStream(parameterIndex: Int, x: Reader): Unit = {
    throw new SQLFeatureNotSupportedException("Character streams are not supported")
  }

  @throws[SQLException]
  override def setCharacterStream(parameterIndex: Int, x: Reader, length: Int): Unit = {
    throw new SQLFeatureNotSupportedException("Character streams are not supported")
  }

  @throws[SQLException]
  override def setCharacterStream(parameterIndex: Int, x: Reader, length: Long): Unit = {
    throw new SQLFeatureNotSupportedException("Character streams are not supported")
  }

  @throws[SQLException]
  override def setClob(parameterIndex: Int, x: Clob): Unit = {
    throw new SQLFeatureNotSupportedException("Clobs are not supported")
  }

  @throws[SQLException]
  override def setClob(parameterIndex: Int, x: Reader): Unit = {
    throw new SQLFeatureNotSupportedException("Clobs are not supported")
  }

  @throws[SQLException]
  override def setClob(parameterIndex: Int, x: Reader, length: Long): Unit = {
    throw new SQLFeatureNotSupportedException("Clobs are not supported")
  }

  @throws[SQLException]
  override def setDate(parameterIndex: Int, x: Date): Unit = {
    setParameter(parameterIndex, TimestampValue(x.getTime))
  }

  @throws[SQLException]
  override def setDate(parameterIndex: Int, x: Date, cal: Calendar): Unit = {
    throw new SQLFeatureNotSupportedException("Dates are not supported")
  }

  @throws[SQLException]
  override def setDouble(parameterIndex: Int, x: Double): Unit = {
    setParameter(parameterIndex, NumericValue(x))
  }

  @throws[SQLException]
  override def setFloat(parameterIndex: Int, x: Float): Unit = {
    setParameter(parameterIndex, NumericValue(BigDecimal(x.toDouble)))
  }

  @throws[SQLException]
  override def setInt(parameterIndex: Int, x: Int): Unit = {
    setParameter(parameterIndex, NumericValue(x))
  }

  @throws[SQLException]
  override def setByte(parameterIndex: Int, x: Byte): Unit = {
    setParameter(parameterIndex, NumericValue(x))
  }

  @throws[SQLException]
  override def setLong(parameterIndex: Int, x: Long): Unit = {
    setParameter(parameterIndex, NumericValue(x))
  }

  @throws[SQLException]
  override def setNCharacterStream(parameterIndex: Int, x: Reader): Unit = {
    throw new SQLFeatureNotSupportedException("Character streams are not supported")
  }

  @throws[SQLException]
  override def setNCharacterStream(parameterIndex: Int, x: Reader, length: Long): Unit = {
    throw new SQLFeatureNotSupportedException("Character streams are not supported")
  }

  @throws[SQLException]
  override def setNClob(parameterIndex: Int, x: NClob): Unit = {
    throw new SQLFeatureNotSupportedException("NClobs are not supported")
  }

  @throws[SQLException]
  override def setNClob(parameterIndex: Int, x: Reader): Unit = {
    throw new SQLFeatureNotSupportedException("NClobs are not supported")
  }

  @throws[SQLException]
  override def setNClob(parameterIndex: Int, x: Reader, length: Long): Unit = {
    throw new SQLFeatureNotSupportedException("NClobs are not supported")
  }

  @throws[SQLException]
  override def setNString(parameterIndex: Int, x: String): Unit = {
    throw new SQLFeatureNotSupportedException("NStrings are not supported")
  }

  @throws[SQLException]
  override def setNull(parameterIndex: Int, sqlType: Int): Unit = {
    throw new SQLFeatureNotSupportedException("nulls are not supported")
  }

  @throws[SQLException]
  override def setNull(parameterIndex: Int, sqlType: Int, typeName: String): Unit = {
    throw new SQLFeatureNotSupportedException("nulls are not supported")
  }

  @throws[SQLException]
  override def setObject(parameterIndex: Int, x: AnyRef): Unit = {
    throw new SQLFeatureNotSupportedException("objects are not supported")
  }

  @throws[SQLException]
  override def setObject(parameterIndex: Int, x: Any, targetSqlType: Int): Unit = {
    throw new SQLFeatureNotSupportedException("objects are not supported")
  }

  @throws[SQLException]
  override def setObject(parameterIndex: Int, x: Any, targetSqlType: Int, scaleOrLength: Int): Unit = {
    throw new SQLFeatureNotSupportedException("objects are not supported")
  }

  @throws[SQLException]
  override def setRef(parameterIndex: Int, x: Ref): Unit = {
    throw new SQLFeatureNotSupportedException("refs are not supported")
  }

  @throws[SQLException]
  override def setRowId(parameterIndex: Int, x: RowId): Unit = {
    throw new SQLFeatureNotSupportedException("Row ids are not supported")
  }

  @throws[SQLException]
  override def setSQLXML(parameterIndex: Int, x: SQLXML): Unit = {
    throw new SQLFeatureNotSupportedException("SQLXMLs are not supported")
  }

  @throws[SQLException]
  override def setShort(parameterIndex: Int, x: Short): Unit = {
    setParameter(parameterIndex, NumericValue(BigDecimal(x)))
  }

  @throws[SQLException]
  override def setString(parameterIndex: Int, x: String): Unit = {
    setParameter(parameterIndex, StringValue(x))
  }

  @throws[SQLException]
  override def setTime(parameterIndex: Int, x: Time): Unit = {
    throw new SQLFeatureNotSupportedException("Time is not supported")
  }

  @throws[SQLException]
  override def setTime(parameterIndex: Int, x: Time, cal: Calendar): Unit = {
    throw new SQLFeatureNotSupportedException("Time is not supported")
  }

  @throws[SQLException]
  override def setTimestamp(parameterIndex: Int, x: Timestamp): Unit = {
    setParameter(parameterIndex, TimestampValue(x.getTime))
  }

  @throws[SQLException]
  override def setTimestamp(parameterIndex: Int, x: Timestamp, cal: Calendar): Unit = {
    throw new SQLFeatureNotSupportedException("Timestamp is not supported")
  }

  @throws[SQLException]
  override def setURL(parameterIndex: Int, x: URL): Unit = {
    throw new SQLFeatureNotSupportedException("URLs are not supported")
  }

  @throws[SQLException]
  override def setUnicodeStream(parameterIndex: Int, x: InputStream, length: Int): Unit = {
    throw new SQLFeatureNotSupportedException("Streams are not supported")
  }
}
