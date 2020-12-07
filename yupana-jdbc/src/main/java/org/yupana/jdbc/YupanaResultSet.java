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

package org.yupana.jdbc;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.function.Function;

import org.joda.time.DateTimeZone;
import org.yupana.api.TypeMeta;

class YupanaResultSet implements ResultSet, ResultSetMetaData {

  public YupanaResultSet(Statement statement, Iterator<Object[]> rows) {
    this.statement = statement;
    this.rows = rows;
    columnNameIndex = new HashMap<>();
    //FIXME: FILL ME
  }

  private final Statement statement;
  private Iterator<Object[]> rows;
  private final Map<String, Integer> columnNameIndex;
  private final TypeMeta[] metas;
  private final String[] columns;
  private final String name;

  private int currentIdx = -1;
  private Object [] currentRow = null;

  private boolean wasNullValue = false;
  private boolean closed = false;

  @Override
  public boolean next() throws SQLException {
    checkClosed();

    if (rows.hasNext()) {
      currentIdx++;
      currentRow = rows.next();
      return true;
    } else {
      currentRow = null;
      return false;
    }
  }

  @Override
  public boolean previous() throws SQLException {
    checkClosed();
    return onlyForwardException();
  }

  @Override
  public void close() throws SQLException {
    closed = true;
    currentRow = null;
    rows = Collections.emptyIterator();
    currentIdx = -1;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return closed;
  }

  @Override
  public Statement getStatement() throws SQLException {
    return statement;
  }


  private boolean onlyForwardException() throws SQLException {
    throw new SQLException("FORWARD_ONLY result set cannot be scrolled back");
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    checkClosed();
    return currentIdx == -1;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    checkClosed();

    return !rows.hasNext() && currentRow == null;
  }

  @Override
  public boolean isFirst() throws SQLException {
    checkClosed();
    return currentIdx == 0;
  }

  @Override
  public boolean isLast() throws SQLException {
    checkClosed();
    return !rows.hasNext() && currentRow != null;
  }

  @Override
  public void beforeFirst() throws SQLException {
    checkClosed();

    if (!isBeforeFirst()) {
      onlyForwardException();
    }
  }

  @Override
  public void afterLast() throws SQLException {
    last();
    next();
  }

  @Override
  public boolean first() throws SQLException {
    checkClosed();

    if (isBeforeFirst()) {
      return next();
    } else if (!isFirst()) {
      return onlyForwardException();
    } else return true;
  }

  @Override
  public boolean last() throws SQLException {
    checkClosed();

    if (isAfterLast()) return onlyForwardException();

    while (rows.hasNext()) {
      currentIdx += 1;
      currentRow = rows.next();
    }
    return true;
  }


  @Override
  public boolean absolute(int row) throws SQLException {
    checkClosed();

    if (row < currentIdx + 1) return onlyForwardException();
    return relative(row - currentIdx);
  }

  @Override
  public boolean relative(int offset) throws SQLException {
    checkClosed();

    if (offset < 0) return onlyForwardException();
    for (int i = 0; i < offset - 1; i++)
      rows.next();
    currentRow = rows.next();
    currentIdx = currentIdx + offset;
    return true;
  }

  @Override
  public int getRow() throws SQLException {
    return currentIdx;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new SQLException("Only FETCH_FORWARD is supported");
    }
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    throw new SQLFeatureNotSupportedException("Fetch size is not supported");
  }

  @Override
  public int getFetchSize() throws SQLException {
    return 0;
  }

  @Override
  public boolean wasNull() throws SQLException {
    checkClosed();
    return wasNullValue;
  }

  @Override
  public int getHoldability() throws SQLException {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public int getType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public int getConcurrency() throws SQLException {
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
  }

  @Override
  public String getCursorName() throws SQLException {
    throw new SQLFeatureNotSupportedException("Cursor names are not supported");
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return this;
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    if (columnNameIndex.containsKey(columnLabel)) {
      return columnNameIndex.get(columnLabel);
    } else {
      throw new SQLException(String.format("Unknown column %s", columnLabel));
    }
  }

  private void checkClosed() throws SQLException {
    if (closed) throw new SQLException("ResultSet is already closed");
  }

  private void checkRow() throws SQLException {
    checkClosed();

    if (currentRow == null) {
      if (currentIdx == -1) {
        throw new SQLException("Trying to read before next call");
      } else {
        throw new SQLException("Reading after the last row");
      }
    }
  }

  private <T> T getValue(int i, T defaultValue) throws SQLException {
    checkRow();
    T cell = (T) currentRow[i - 1];

    wasNullValue = cell == null;
    if (cell == null) {
      return defaultValue;
    } else {
      return cell;
    }
  }

  private <T> Optional<T> getConvertedValue(int i, SqlLambda<Object, T> mapper) throws SQLException {
    checkRow();
    Object cell = currentRow[i - 1];

    wasNullValue = cell == null;
    if (cell == null) {
      return Optional.empty();
    } else {
      return Optional.of(mapper.apply(cell));
    }
  }

  private <T> T getValue(int i) throws SQLException {
    return getValue(i, null);
  }

  private <T> T getValue(String name, T defaultValue) throws SQLException {
    return getValue(columnNameIndex.get(name), defaultValue);
  }

  private <T> Optional<T> getConvertedValue(String name, SqlLambda<Object, T> mapper) throws SQLException {
    return getConvertedValue(columnNameIndex.get(name), mapper);
  }

  private <T> T getValue(String name) throws SQLException {
    return getValue(columnNameIndex.get(name));
  }

//  private def getPrimitiveByName[T <: AnyVal](name: String, default: T): T = {
//    getPrimitive(columnNameIndex(name), default)
//  }
//
//  private def getReferenceByName[T >: Null](name: String, f: Any => T): T = {
//    getReference(columnNameIndex(name), f)
//  }
//
//  private def getReference[T >: Null](i: Int): T = {
//    getReference(i, _.asInstanceOf[T])
//  }
//
//  private def getReferenceByName[T >: Null](name: String): T = {
//    getReference(columnNameIndex(name))
//  }
//
//  private def toBigDecimal(a: Any): BigDecimal = a.asInstanceOf[scala.math.BigDecimal].underlying()
//
//  private static long toLocalMillis(Object a) throws SQLException {
//    if (a instanceof Timestamp) {
//      Timestamp t = (Timestamp) a;
//      return DateTimeZone.getDefault().convertLocalToUTC(t.getTime(), false);
//    } else {
//      throw new SQLException(String.format("Cannot cast %s to Time", a));
//    }
//  }

  private <T> T toCalendarMillis(Timestamp t, Calendar c, Function<Long, T> mapper) throws SQLException {
    if (t == null) return  null;
    return mapper.apply(DateTimeZone.forTimeZone(c.getTimeZone()).convertLocalToUTC(t.getTime(), false));
  }

  private static byte[] toBytes(Object a) throws SQLException {
    ByteArrayOutputStream bs = new ByteArrayOutputStream();
    try {
      ObjectOutputStream os = new ObjectOutputStream(bs);
      os.writeObject(a);
    } catch (IOException e) {
      throw new SQLException("Failed to write object", e);
    }
    return bs.toByteArray();
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    return getConvertedValue(columnIndex, Object::toString).orElse(null);
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    return getConvertedValue(columnLabel, Object::toString).orElse(null);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    return getValue(columnIndex, false);
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    return getValue(columnIndex, (byte)0);
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    return getValue(columnLabel, (byte) 0);
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    return getValue(columnIndex, (short)0);
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    return getValue(columnLabel, (short)0);
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    return getValue(columnIndex, 0);
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    return getValue(columnLabel, 0);
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    return getValue(columnIndex, 0L);
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    return getValue(columnLabel, 0L);
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    return getValue(columnIndex, (float)0);
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    return getValue(columnLabel, (float) 0);
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    return getValue(columnIndex, 0d);
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    return getValue(columnLabel, 0d);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    return getConvertedValue(columnIndex, o -> ((BigDecimal)o).setScale(scale, RoundingMode.UNNECESSARY)).orElse(null);
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    return getConvertedValue(columnLabel, o -> ((BigDecimal)o).setScale(scale, RoundingMode.UNNECESSARY)).orElse(null);
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    JdbcUtils.checkTypeMapping(map);
    return getObject(columnIndex);
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    JdbcUtils.checkTypeMapping(map);
    return getObject(columnLabel);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    return toCalendarMillis(getTimestamp(columnIndex), cal, Timestamp::new);
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    return toCalendarMillis(getTimestamp(columnLabel), cal, Timestamp::new);
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    return new Date(getTimestamp(columnIndex).getTime());
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    return new Date(getTimestamp(columnLabel).getTime());
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    return toCalendarMillis(getTimestamp(columnIndex), cal, Date::new);
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    return toCalendarMillis(getTimestamp(columnLabel), cal, Date::new);
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    return new Time(getTimestamp(columnIndex).getTime());
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    return new Time(getTimestamp(columnLabel).getTime());
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    return toCalendarMillis(getTimestamp(columnIndex), cal, Time::new);
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    return toCalendarMillis(getTimestamp(columnLabel), cal, Time::new);
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    return getConvertedValue(columnIndex, YupanaResultSet::toBytes).orElse(null);
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    return getConvertedValue(columnLabel, YupanaResultSet::toBytes).orElse(null);
  }

  private InputStream toTextStream(String s, Charset charset) {
    if (s == null) return null;
    return new ByteArrayInputStream(s.getBytes(charset));
  }

  private Reader toCharStream(String s) {
    if (s == null) return null;
    return new CharArrayReader(s.toCharArray());
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    return toTextStream(getString(columnIndex), StandardCharsets.US_ASCII);
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    return toTextStream(getString(columnLabel), StandardCharsets.US_ASCII);
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    return toTextStream(getString(columnIndex), StandardCharsets.UTF_8);
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    return return toTextStream(getString(columnLabel), StandardCharsets.UTF_8);;
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    return toCharStream(getString(columnIndex));
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    return toCharStream(getString(columnLabel));
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    return getValue(columnIndex);
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    return getValue(columnLabel);
  }

  //  private def createArray(i: Int, name: String, v: Any): YupanaArray[_] = {
//    val dt = dataTypes(i - 1)
//    if (dt.isArray) {
//      val dtt = dt.asInstanceOf[ArrayDataType[_]]
//      new YupanaArray(name, v.asInstanceOf[Seq[dtt.valueType.T]].toArray, dtt.valueType.meta)
//    } else {
//      throw new SQLException(s"$dt is not an array")
//    }
//  }
//
//  private YupanaBlob createBlob(int i, Object v) {
//    import org.yupana.api.{Blob => ApiBlob}
//    val dt = dataTypes(i - 1)
//    if (dt.meta.sqlType == Types.BLOB) {
//      new YupanaBlob(v.asInstanceOf[ApiBlob].bytes)
//    } else {
//      throw new SQLException(s"$dt is not a blob")
//    }
//  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Refs are not supported");
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("Refs are not supported");
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("CLOBs are not supported");
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("CLOBs are not supported");
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("URLs are not supported");
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("URLs are not supported");
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("NClobs are not supported");
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("NClobs are not supported");
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("NStrings are not supported");
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("NStrings are not supported");
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("NCharacterStreams are not supported");
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("NCharacterStreams are not supported");
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("RowIds is not supported");
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("RowId is not supported");
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("SQLXML is not supported");
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("SQLXML is not supported");
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.getObject(int, Class<T>)");
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.getObject(String, Class<T>)");
  }

  @Override
  public int getColumnCount() throws SQLException {
    return metas.length;
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    return metas[column - 1].getSqlType();
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    return metas[column - 1].getSqlTypeName();
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    return metas[column - 1].getJavaTypeName();
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    return metas[column - 1].getPrecision();;
  }

  @Override
  public int getScale(int column) throws SQLException {
    return metas[column - 1].getScale();
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    return metas[column - 1].getDisplaySize();
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    return columns[column - 1];
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return columns[column - 1];
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    return name;
  }

  @Override
  public String getTableName(int column) throws SQLException {
    return name;
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    return metas[column - 1].isSigned();
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    return metas[column - 1].getSqlType() == Types.VARCHAR;
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    return false;
  }

  @Override
  public int isNullable(int column) throws SQLException {
    return ResultSetMetaData.columnNullable;
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    return "";
  }

  @Override
  public void insertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void deleteRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void refreshRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.rowUpdated()");
  }

  @Override
  public boolean rowInserted() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.rowInserted()");
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported: ResultSet.rowDeleted()");
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
    throw new SQLFeatureNotSupportedException("This result set is read only");
  }

  @Override
  public <T> T unwrap(Class<T> aClass) throws SQLException {
    if (!aClass.isAssignableFrom(getClass())) {
      throw new SQLException(String.format("Cannot unwrap to %s", aClass.getName()));
    }

    return aClass.cast(this);
  }

  @Override
  public boolean isWrapperFor(Class<?> aClass) throws SQLException {
    return aClass.isAssignableFrom(getClass());
  }
}
