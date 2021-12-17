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

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, CharArrayReader, InputStream, ObjectOutputStream, Reader }
import java.math.BigDecimal
import java.nio.charset.{ Charset, StandardCharsets }
import java.sql.{ Array => SqlArray, _ }
import java.util
import java.util.Calendar
import org.yupana.api.query.{ DataRow, Result }
import org.yupana.api.types.ArrayDataType
import org.yupana.api.types.DataType.TypeKind
import org.yupana.api.{ Time => ApiTime }
import org.yupana.jdbc.compat.LazyList

import java.time.{ LocalDateTime, ZonedDateTime }

class YupanaResultSet protected[jdbc] (
    statement: Statement,
    result: Result
) extends ResultSet
    with ResultSetMetaData {

  private val columnNameIndex = result.fieldNames.zip(LazyList.from(1)).toMap
  private val columns = result.fieldNames.toArray
  private val dataTypes = result.dataTypes.toArray

  private var currentIdx = -1
  private var it = result.iterator
  private var currentRow: DataRow = _

  private var wasNullValue = false
  private var closed = false

  @throws[SQLException]
  override def getStatement: Statement = statement

  @throws[SQLException]
  override def next: Boolean = {
    checkClosed()

    if (it.hasNext) {
      currentIdx += 1
      currentRow = it.next()
      true
    } else {
      currentRow = null
      false
    }
  }

  private def onlyForwardException(): Boolean =
    throw new SQLException("FORWARD_ONLY result set cannot be scrolled back")

  @throws[SQLException]
  override def isBeforeFirst: Boolean = {
    checkClosed()

    currentIdx == -1
  }

  @throws[SQLException]
  override def isAfterLast: Boolean = {
    checkClosed()

    !it.hasNext && currentRow == null
  }

  @throws[SQLException]
  override def isFirst: Boolean = {
    checkClosed()
    currentIdx == 0
  }

  @throws[SQLException]
  override def isLast: Boolean = {
    checkClosed()
    !it.hasNext && currentRow != null
  }

  @throws[SQLException]
  override def beforeFirst(): Unit = {
    checkClosed()

    if (!isBeforeFirst) {
      onlyForwardException()
    }
  }

  @throws[SQLException]
  override def afterLast(): Unit = {
    last()
    next()
  }

  @throws[SQLException]
  override def first: Boolean = {
    checkClosed()

    if (isBeforeFirst) {
      next()
    } else if (!isFirst) {
      onlyForwardException()
    } else true
  }

  @throws[SQLException]
  override def last: Boolean = {
    checkClosed()

    if (isAfterLast) onlyForwardException()

    while (it.hasNext) {
      currentIdx += 1
      currentRow = it.next()
    }
    true
  }

  @throws[SQLException]
  override def getRow: Int = {
    checkClosed()

    currentIdx + 1
  }

  @throws[SQLException]
  override def absolute(row: Int): Boolean = {
    checkClosed()

    if (row < currentIdx + 1) onlyForwardException()
    it = result.iterator.drop(row - currentIdx - 2)
    currentRow = it.next()
    currentIdx = row - 1
    true
  }

  @throws[SQLException]
  override def relative(rows: Int): Boolean = {
    checkClosed()

    if (rows < 0) onlyForwardException()
    it = result.iterator.drop(rows - 1)
    currentRow = it.next()
    currentIdx = currentIdx + rows
    true
  }

  @throws[SQLException]
  override def previous: Boolean = {
    checkClosed()

    onlyForwardException()
  }

  @throws[SQLException]
  override def setFetchDirection(direction: Int): Unit = {
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new SQLException("Only FETCH_FORWARD is supported")
    }
  }

  @throws[SQLException]
  override def getFetchDirection: Int = ResultSet.FETCH_FORWARD

  @throws[SQLException]
  override def setFetchSize(i: Int): Unit = throw new SQLFeatureNotSupportedException("Fetch size is not supported")

  @throws[SQLException]
  override def getFetchSize: Int = 0

  @throws[SQLException]
  override def close(): Unit = {
    closed = true
    currentRow = null
    it = Iterator.empty
    currentIdx = -1
  }

  @throws[SQLException]
  override def isClosed: Boolean = closed

  @throws[SQLException]
  override def wasNull: Boolean = {
    checkClosed()
    wasNullValue
  }

  @throws[SQLException]
  override def getHoldability: Int = ResultSet.HOLD_CURSORS_OVER_COMMIT

  @throws[SQLException]
  override def clearWarnings(): Unit = {}

  @throws[SQLException]
  override def getCursorName: String = {
    throw new SQLFeatureNotSupportedException("Cursor names are not supported")
  }

  @throws[SQLException]
  override def getMetaData: ResultSetMetaData = this

  @throws[SQLException]
  override def findColumn(s: String): Int = columnNameIndex.getOrElse(s, throw new SQLException(s"Unknown column $s"))

  private def checkClosed(): Unit = {
    if (closed) throw new SQLException("ResultSet is already closed")
  }

  private def checkRow(): Unit = {
    checkClosed()

    if (currentRow == null) {
      if (currentIdx == -1) {
        throw new SQLException("Trying to read before next call")
      } else {
        throw new SQLException("Reading after the last row")
      }
    }
  }

  private def getPrimitive[T <: AnyVal](i: Int, default: T): T = {
    checkRow()
    val cell = currentRow.get[T](i - 1)
    wasNullValue = cell == null
    if (cell == null) {
      default
    } else {
      cell
    }
  }

  private def getPrimitiveByName[T <: AnyVal](name: String, default: T): T = {
    getPrimitive(columnNameIndex(name), default)
  }

  private def getReference[T >: Null](i: Int, f: Any => T): T = {
    checkRow()
    val cell = currentRow.get[T](i - 1)

    wasNullValue = cell == null
    if (cell == null) {
      cell
    } else {
      f(cell)
    }
  }

  private def getReferenceByName[T >: Null](name: String, f: Any => T): T = {
    getReference(columnNameIndex(name), f)
  }

  private def getReference[T >: Null](i: Int): T = {
    getReference(i, _.asInstanceOf[T])
  }

  private def getReferenceByName[T >: Null](name: String): T = {
    getReference(columnNameIndex(name))
  }

  private def toBigDecimal(a: Any): BigDecimal = a.asInstanceOf[scala.math.BigDecimal].underlying()

  private def toLocalDateTime(a: Any): LocalDateTime = {
    a match {
      case t: ApiTime => t.toLocalDateTime
      case x          => throw new SQLException(s"Cannot cast $x to Time")
    }
  }

  private def toZonedDateTime(a: Any, c: Calendar): ZonedDateTime = {
    a match {
      case t: ApiTime => t.toLocalDateTime.atZone(c.getTimeZone.toZoneId)
      case x          => throw new SQLException(s"Cannot cast $x to Time")
    }
  }

  private def toBytes(a: Any): Array[Byte] = {
    val bs = new ByteArrayOutputStream()
    val os = new ObjectOutputStream(bs)
    os.writeObject(a)
    bs.toByteArray
  }

  @throws[SQLException]
  override def getString(i: Int): String = getReference(i, _.toString)

  @throws[SQLException]
  override def getBoolean(i: Int): Boolean = getPrimitive(i, false)

  @throws[SQLException]
  override def getByte(i: Int): Byte = getPrimitive(i, 0)

  @throws[SQLException]
  override def getShort(i: Int): Short = getPrimitive(i, 0)

  @throws[SQLException]
  override def getInt(i: Int): Int = getPrimitive(i, 0)

  @throws[SQLException]
  override def getLong(i: Int): Long = getPrimitive(i, 0L)

  @throws[SQLException]
  override def getFloat(i: Int): Float = getPrimitive(i, 0f)

  @throws[SQLException]
  override def getDouble(i: Int): Double = getPrimitive(i, 0d)

  @throws[SQLException]
  override def getBigDecimal(i: Int, scale: Int): BigDecimal = getReference(i, x => toBigDecimal(x).setScale(scale))

  @throws[SQLException]
  override def getBytes(i: Int): Array[Byte] = getReference(i, toBytes)

  @throws[SQLException]
  override def getBytes(s: String): Array[Byte] = getReferenceByName(s, toBytes)

  @throws[SQLException]
  override def getDate(i: Int): Date = getReference(i, a => Date.valueOf(toLocalDateTime(a).toLocalDate))

  @throws[SQLException]
  override def getTime(i: Int): Time = getReference(i, a => Time.valueOf(toLocalDateTime(a).toLocalTime))

  @throws[SQLException]
  override def getTimestamp(i: Int): Timestamp = getReference(i, a => Timestamp.valueOf(toLocalDateTime(a)))

  private def toTextStream(s: String, charset: Charset): InputStream = {
    if (s != null) {
      new ByteArrayInputStream(s.getBytes(charset))
    } else null
  }

  @throws[SQLException]
  override def getAsciiStream(i: Int): InputStream = toTextStream(getString(i), StandardCharsets.US_ASCII)

  @throws[SQLException]
  override def getAsciiStream(s: String): InputStream = toTextStream(getString(s), StandardCharsets.US_ASCII)

  @throws[SQLException]
  override def getUnicodeStream(i: Int): InputStream = toTextStream(getString(i), StandardCharsets.UTF_8)

  @throws[SQLException]
  override def getUnicodeStream(s: String): InputStream = toTextStream(getString(s), StandardCharsets.UTF_8)

  private def toCharStream(s: String): Reader = {
    if (s != null) {
      new CharArrayReader(s.toCharArray)
    } else null
  }

  @throws[SQLException]
  override def getCharacterStream(i: Int): Reader = toCharStream(getString(i))

  @throws[SQLException]
  override def getCharacterStream(s: String): Reader = toCharStream(getString(s))

  @throws[SQLException]
  override def getBinaryStream(s: String): InputStream = null

  @throws[SQLException]
  override def getBinaryStream(i: Int): InputStream = null

  @throws[SQLException]
  override def getString(s: String): String = getReferenceByName(s, _.toString)

  @throws[SQLException]
  override def getBoolean(s: String): Boolean = getPrimitiveByName(s, false)

  @throws[SQLException]
  override def getByte(s: String): Byte = getPrimitiveByName(s, 0)

  @throws[SQLException]
  override def getShort(s: String): Short = getPrimitiveByName(s, 0)

  @throws[SQLException]
  override def getInt(s: String): Int = getPrimitiveByName(s, 0)

  @throws[SQLException]
  override def getLong(s: String): Long = getPrimitiveByName(s, 0L)

  @throws[SQLException]
  override def getFloat(s: String): Float = getPrimitiveByName(s, 0f)

  @throws[SQLException]
  override def getDouble(s: String): Double = getPrimitiveByName(s, 0d)

  @throws[SQLException]
  override def getBigDecimal(s: String, scale: Int): BigDecimal =
    getReferenceByName(s, x => toBigDecimal(x).setScale(scale))

  @throws[SQLException]
  override def getDate(s: String): Date = getReferenceByName(s, a => Date.valueOf(toLocalDateTime(a).toLocalDate))

  @throws[SQLException]
  override def getTime(s: String): Time = getReferenceByName(s, a => Time.valueOf(toLocalDateTime(a).toLocalTime))

  @throws[SQLException]
  override def getTimestamp(s: String): Timestamp = getReferenceByName(s, a => Timestamp.valueOf(toLocalDateTime(a)))

  @throws[SQLException]
  override def getObject(i: Int): AnyRef = getReference(i)

  @throws[SQLException]
  override def getObject(s: String): AnyRef = getReferenceByName(s)

  @throws[SQLException]
  override def getBigDecimal(i: Int): BigDecimal = getReference(i, toBigDecimal)

  @throws[SQLException]
  override def getBigDecimal(s: String): BigDecimal = getReferenceByName(s, toBigDecimal)

  @throws[SQLException]
  override def getWarnings: SQLWarning = null

  @throws[SQLException]
  override def getType: Int = ResultSet.TYPE_FORWARD_ONLY

  @throws[SQLException]
  override def getConcurrency: Int = ResultSet.CONCUR_READ_ONLY

  @throws[SQLException]
  override def rowUpdated = throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.rowUpdated()")

  @throws[SQLException]
  override def rowInserted = throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.rowInserted()")

  @throws[SQLException]
  override def rowDeleted = throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.rowDeleted()")

  @throws[SQLException]
  override def getObject(i: Int, map: util.Map[String, Class[_]]): AnyRef = {
    JdbcUtils.checkTypeMapping(map)
    getObject(i)
  }

  @throws[SQLException]
  override def getRef(i: Int) = throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.getRef(int)")

  @throws[SQLException]
  override def getClob(i: Int) =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.getClob(int)")

  private def createArray(i: Int, name: String, v: Any): YupanaArray[_] = {
    val dt = dataTypes(i - 1)
    if (dt.kind == TypeKind.Array) {
      val dtt = dt.asInstanceOf[ArrayDataType[_]]
      new YupanaArray(name, v.asInstanceOf[Seq[dtt.valueType.T]].toArray, dtt.valueType)
    } else {
      throw new SQLException(s"$dt is not an array")
    }
  }

  private def createBlob(i: Int, v: Any): YupanaBlob = {
    import org.yupana.api.{ Blob => ApiBlob }
    val dt = dataTypes(i - 1)
    if (dt.meta.sqlType == Types.BLOB) {
      new YupanaBlob(v.asInstanceOf[ApiBlob].bytes)
    } else {
      throw new SQLException(s"$dt is not a blob")
    }
  }

  @throws[SQLException]
  override def getArray(i: Int): SqlArray = {
    getReference(i, v => createArray(i, columns(i), v))
  }

  @throws[SQLException]
  override def getArray(colName: String): SqlArray = {
    val idx = columnNameIndex(colName)
    getReference(idx, v => createArray(idx, colName, v))
  }

  @throws[SQLException]
  override def getBlob(i: Int): Blob = {
    getReference(i, v => createBlob(i, v))
  }

  @throws[SQLException]
  override def getBlob(colName: String): Blob = {
    val idx = columnNameIndex(colName)
    getBlob(idx)
  }

  @throws[SQLException]
  override def getObject(colName: String, map: util.Map[String, Class[_]]): AnyRef = {
    JdbcUtils.checkTypeMapping(map)
    getObject(colName)
  }

  @throws[SQLException]
  override def getRef(colName: String) =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.getRef(String)")

  @throws[SQLException]
  override def getClob(colName: String) =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.getClob(String)")

  @throws[SQLException]
  override def getDate(columnIndex: Int, cal: Calendar): Date =
    getReference(columnIndex, a => Date.valueOf(toZonedDateTime(a, cal).toLocalDate))

  @throws[SQLException]
  override def getDate(columnName: String, cal: Calendar): Date =
    getReferenceByName(columnName, a => Date.valueOf(toZonedDateTime(a, cal).toLocalDate))

  @throws[SQLException]
  override def getTime(columnIndex: Int, cal: Calendar): Time =
    getReference(columnIndex, a => Time.valueOf(toZonedDateTime(a, cal).toLocalTime))

  @throws[SQLException]
  override def getTime(columnName: String, cal: Calendar): Time =
    getReferenceByName(columnName, a => Time.valueOf(toZonedDateTime(a, cal).toLocalTime))

  @throws[SQLException]
  override def getTimestamp(columnIndex: Int, cal: Calendar): Timestamp = {
    getReference(columnIndex, a => new Timestamp(toZonedDateTime(a, cal).toInstant.toEpochMilli))
  }

  @throws[SQLException]
  override def getTimestamp(columnName: String, cal: Calendar): Timestamp =
    getReferenceByName(columnName, a => new Timestamp(toZonedDateTime(a, cal).toInstant.toEpochMilli))

  @throws[SQLException]
  override def getURL(i: Int) = throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.getURL(int)")

  @throws[SQLException]
  override def getURL(s: String) =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.getURL(String)")

  @throws[SQLException]
  override def getNCharacterStream(columnIndex: Int): Reader =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.getNCharacterStream")

  @throws[SQLException]
  override def getNCharacterStream(columnLabel: String): Reader =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.getNCharacterStream")

  @throws[SQLException]
  override def getNString(columnIndex: Int): String =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.getNString")

  @throws[SQLException]
  override def getNString(columnLabel: String): String =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.getNString")

  @throws[SQLException]
  override def getNClob(arg0: Int): NClob = throw new SQLFeatureNotSupportedException("NClob is not supported")

  @throws[SQLException]
  override def getNClob(arg0: String): NClob = throw new SQLFeatureNotSupportedException("NClob is not supported")

  @throws[SQLException]
  override def getRowId(arg0: Int): RowId = throw new SQLFeatureNotSupportedException("RowId is not supported")

  @throws[SQLException]
  override def getRowId(arg0: String): RowId = throw new SQLFeatureNotSupportedException("RowId is not supported")

  @throws[SQLException]
  override def getSQLXML(arg0: Int): SQLXML = throw new SQLFeatureNotSupportedException("SQLXML is not supported")

  @throws[SQLException]
  override def getSQLXML(arg0: String): SQLXML = throw new SQLFeatureNotSupportedException("SQLXML is not supported")

  @throws[SQLException]
  override def updateNull(columnIndex: Int): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateNull(int)")

  @throws[SQLException]
  override def updateBoolean(columnIndex: Int, x: Boolean): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateBoolean(int, boolean)")

  @throws[SQLException]
  override def updateByte(columnIndex: Int, x: Byte): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateByte(int, byte)")

  @throws[SQLException]
  override def updateShort(columnIndex: Int, x: Short): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateShort(int, short)")

  @throws[SQLException]
  override def updateInt(columnIndex: Int, x: Int): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateInt(int, int)")

  @throws[SQLException]
  override def updateLong(columnIndex: Int, x: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateLong(int, long)")

  @throws[SQLException]
  override def updateFloat(columnIndex: Int, x: Float): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateFloat(int, float)")

  @throws[SQLException]
  override def updateDouble(columnIndex: Int, x: Double): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateDouble(int, double)")

  @throws[SQLException]
  override def updateBigDecimal(columnIndex: Int, x: BigDecimal): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateBigDecimal(int, BigDecimal)")

  @throws[SQLException]
  override def updateString(columnIndex: Int, x: String): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateString(int, String)")

  @throws[SQLException]
  override def updateBytes(columnIndex: Int, x: Array[Byte]): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateBytes(int, byte[])")

  @throws[SQLException]
  override def updateDate(columnIndex: Int, x: Date): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateDate(int, Date)")

  @throws[SQLException]
  override def updateTime(columnIndex: Int, x: Time): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateTime(int, Time)")

  @throws[SQLException]
  override def updateTimestamp(columnIndex: Int, x: Timestamp): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateTimestamp(int, Timestamp)")

  @throws[SQLException]
  override def updateAsciiStream(columnIndex: Int, x: InputStream, length: Int): Unit =
    throw new SQLFeatureNotSupportedException(
      "Method not supported: ResultSet.updateAsciiStream(int, InputStream, int)"
    )

  @throws[SQLException]
  override def updateBinaryStream(columnIndex: Int, x: InputStream, length: Int): Unit =
    throw new SQLFeatureNotSupportedException(
      "Method not supported: ResultSet.updateBinaryStreamint, InputStream, int)"
    )

  @throws[SQLException]
  override def updateCharacterStream(columnIndex: Int, x: Reader, length: Int): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateCharacterStream(int, Reader, int)")

  @throws[SQLException]
  override def updateObject(columnIndex: Int, x: Any, scale: Int): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.udpateObject(int, Object)")

  @throws[SQLException]
  override def updateObject(columnIndex: Int, x: Any): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateObject(int, Object, int)")

  @throws[SQLException]
  override def updateNull(columnName: String): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateNull(String)")

  @throws[SQLException]
  override def updateBoolean(columnName: String, x: Boolean): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateBoolean(String, boolean)")

  @throws[SQLException]
  override def updateByte(columnName: String, x: Byte): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateByte(String, byte)")

  @throws[SQLException]
  override def updateShort(columnName: String, x: Short): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateShort(String, short)")

  @throws[SQLException]
  override def updateInt(columnName: String, x: Int): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateInt(String, int)")

  @throws[SQLException]
  override def updateLong(columnName: String, x: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateLong(String, long)")

  @throws[SQLException]
  override def updateFloat(columnName: String, x: Float): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateFloat(String, float)")

  @throws[SQLException]
  override def updateDouble(columnName: String, x: Double): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateDouble(String, double)")

  @throws[SQLException]
  override def updateBigDecimal(columnName: String, x: BigDecimal): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateBigDecimal(String, BigDecimal)")

  @throws[SQLException]
  override def updateString(columnName: String, x: String): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateString(String, String)")

  @throws[SQLException]
  override def updateBytes(columnName: String, x: Array[Byte]): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateBytes(String, byte[])")

  @throws[SQLException]
  override def updateDate(columnName: String, x: Date): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateDate(String, Date)")

  @throws[SQLException]
  override def updateTime(columnName: String, x: Time): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateTime(String, Time)")

  @throws[SQLException]
  override def updateTimestamp(columnName: String, x: Timestamp): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateTimestamp(String, Timestamp)")

  @throws[SQLException]
  override def updateAsciiStream(columnName: String, x: InputStream, length: Int): Unit =
    throw new SQLFeatureNotSupportedException(
      "Method not supported: ResultSet.updateAsciiStream(String, InputStream, int)"
    )

  @throws[SQLException]
  override def updateBinaryStream(columnName: String, x: InputStream, length: Int): Unit =
    throw new SQLFeatureNotSupportedException(
      "Method not supported: ResultSet.updateBinaryStream(String, InputStream, int)"
    )

  @throws[SQLException]
  override def updateCharacterStream(columnName: String, reader: Reader, length: Int): Unit =
    throw new SQLFeatureNotSupportedException(
      "Method not supported: ResultSet.updateCharacterStream(String, Reader, int)"
    )

  @throws[SQLException]
  override def updateObject(columnName: String, x: Any, scale: Int): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateObject(String, Object, int)")

  @throws[SQLException]
  override def updateObject(columnName: String, x: Any): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateObject(String, Object)")

  @throws[SQLException]
  override def insertRow(): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.insertRow()")

  @throws[SQLException]
  override def updateRow(): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateRow()")

  @throws[SQLException]
  override def deleteRow(): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.deleteRow()")

  @throws[SQLException]
  override def refreshRow(): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.refreshRow()")

  @throws[SQLException]
  override def cancelRowUpdates(): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.cancelRowUpdates()")

  @throws[SQLException]
  override def moveToInsertRow(): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.moveToInsertRow()")

  @throws[SQLException]
  override def moveToCurrentRow(): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.moveToeCurrentRow()")

  @throws[SQLException]
  override def updateRef(columnIndex: Int, x: Ref): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateRef(int,java.sql.Ref)")

  @throws[SQLException]
  override def updateRef(columnName: String, x: Ref): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateRef(String,java.sql.Ref)")

  @throws[SQLException]
  override def updateBlob(columnIndex: Int, x: Blob): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateBlob(int,java.sql.Blob)")

  @throws[SQLException]
  override def updateBlob(columnName: String, x: Blob): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateBlob(String,java.sql.Blob)")

  @throws[SQLException]
  override def updateClob(columnIndex: Int, x: Clob): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateClob(int,java.sql.Clob)")

  @throws[SQLException]
  override def updateClob(columnName: String, x: Clob): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateClob(String,java.sql.Clob)")

  @throws[SQLException]
  override def updateArray(columnIndex: Int, x: SqlArray): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateArray(int,java.sql.Array)")

  @throws[SQLException]
  override def updateArray(columnName: String, x: SqlArray): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateArray(String,java.sql.Array)")

  @throws[SQLException]
  override def updateAsciiStream(columnIndex: Int, x: InputStream): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateAsciiStream")

  @throws[SQLException]
  override def updateAsciiStream(columnLabel: String, x: InputStream): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateAsciiStream")

  @throws[SQLException]
  override def updateAsciiStream(columnIndex: Int, x: InputStream, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateAsciiStream")

  @throws[SQLException]
  override def updateAsciiStream(columnLabel: String, x: InputStream, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateAsciiStream")

  @throws[SQLException]
  override def updateBinaryStream(columnIndex: Int, x: InputStream): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateBinaryStream")

  @throws[SQLException]
  override def updateBinaryStream(columnLabel: String, x: InputStream): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateBinaryStream")

  @throws[SQLException]
  override def updateBinaryStream(columnIndex: Int, x: InputStream, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateBinaryStream")

  @throws[SQLException]
  override def updateBinaryStream(columnLabel: String, x: InputStream, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateBinaryStream")

  @throws[SQLException]
  override def updateBlob(columnIndex: Int, inputStream: InputStream): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateBlob")

  @throws[SQLException]
  override def updateBlob(columnLabel: String, inputStream: InputStream): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateBlob")

  @throws[SQLException]
  override def updateBlob(columnIndex: Int, inputStream: InputStream, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateBlob")

  @throws[SQLException]
  override def updateBlob(columnLabel: String, inputStream: InputStream, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateBlob")

  @throws[SQLException]
  override def updateCharacterStream(columnIndex: Int, x: Reader): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateCharacterStream")

  @throws[SQLException]
  override def updateCharacterStream(columnLabel: String, reader: Reader): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateCharacterStream")

  @throws[SQLException]
  override def updateCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateCharacterStream")

  @throws[SQLException]
  override def updateCharacterStream(columnLabel: String, reader: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateCharacterStream")

  @throws[SQLException]
  override def updateClob(columnIndex: Int, reader: Reader): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateClob")

  @throws[SQLException]
  override def updateClob(columnLabel: String, reader: Reader): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateClob")

  @throws[SQLException]
  override def updateClob(columnIndex: Int, reader: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateClob")

  @throws[SQLException]
  override def updateClob(columnLabel: String, reader: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateClob")

  @throws[SQLException]
  override def updateNCharacterStream(columnIndex: Int, x: Reader): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateNCharacterStream")

  @throws[SQLException]
  override def updateNCharacterStream(columnLabel: String, reader: Reader): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateNCharacterStream")

  @throws[SQLException]
  override def updateNCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateNCharacterStream")

  @throws[SQLException]
  override def updateNCharacterStream(columnLabel: String, reader: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateNCharacterStream")

  @throws[SQLException]
  override def updateNClob(columnIndex: Int, reader: Reader): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateNClob")

  @throws[SQLException]
  override def updateNClob(columnLabel: String, reader: Reader): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateNClob")

  @throws[SQLException]
  override def updateNClob(columnIndex: Int, reader: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateNClob")

  @throws[SQLException]
  override def updateNClob(columnLabel: String, reader: Reader, length: Long): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateNClob")

  @throws[SQLException]
  override def updateNString(columnIndex: Int, string: String): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateNString")

  @throws[SQLException]
  override def updateNString(columnLabel: String, string: String): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateNString")

  @throws[SQLException]
  override def updateNClob(arg0: Int, arg1: NClob): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateNClob")

  @throws[SQLException]
  override def updateNClob(arg0: String, arg1: NClob): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateNClob")

  @throws[SQLException]
  override def updateRowId(arg0: Int, arg1: RowId): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateRowId")

  @throws[SQLException]
  override def updateRowId(arg0: String, arg1: RowId): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateRowId")

  @throws[SQLException]
  override def updateSQLXML(arg0: Int, arg1: SQLXML): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateSQLXML")

  @throws[SQLException]
  override def updateSQLXML(arg0: String, arg1: SQLXML): Unit =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.updateSQLXML")

  @throws[SQLException]
  override def isWrapperFor(aClass: Class[_]): Boolean = aClass.isAssignableFrom(getClass)

  @throws[SQLException]
  override def unwrap[T](aClass: Class[T]): T = {
    if (!aClass.isAssignableFrom(getClass)) {
      throw new SQLException(s"Cannot unwrap to ${aClass.getName}")
    }

    aClass.cast(this)
  }

  @throws[SQLException]
  override def getObject[T](columnLabel: String, `type`: Class[T]) =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.getObject(String, Class<T>)")

  @throws[SQLException]
  override def getObject[T](columnIndex: Int, `type`: Class[T]) =
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.getObject(int, Class<T>)")

  override def getColumnLabel(column: Int): String = columns(column - 1)

  override def isCaseSensitive(column: Int): Boolean = false

  override def getTableName(column: Int): String = result.name

  override def isDefinitelyWritable(column: Int): Boolean = false

  override def getColumnDisplaySize(column: Int): Int = dataTypes(column - 1).meta.displaySize

  override def getSchemaName(column: Int): String = result.name

  override def getColumnName(column: Int): String = columns(column - 1)

  override def isWritable(column: Int) = false

  override def isAutoIncrement(column: Int) = false

  override def isReadOnly(column: Int) = true

  override def isCurrency(column: Int) = false

  override def getColumnType(column: Int): Int = dataTypes(column - 1).meta.sqlType

  override def isSigned(column: Int): Boolean = dataTypes(column - 1).meta.isSigned

  override def getColumnTypeName(column: Int): String = dataTypes(column - 1).meta.sqlTypeName

  override def getColumnClassName(column: Int): String = dataTypes(column - 1).meta.javaTypeName

  override def getColumnCount: Int = columns.length

  override def getPrecision(column: Int): Int = dataTypes(column - 1).meta.precision

  override def getScale(column: Int): Int = dataTypes(column - 1).meta.scale

  override def isNullable(column: Int): Int = ResultSetMetaData.columnNullable

  override def getCatalogName(column: Int) = ""

  override def isSearchable(column: Int) = true
}
