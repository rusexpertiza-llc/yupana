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

import org.yupana.Proto;
import org.yupana.api.query.Result;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

class YupanaPreparedStatement extends YupanaStatement implements PreparedStatement {

    public YupanaPreparedStatement(YupanaConnection connection, String templateQuery) {
        super(connection);
        this.templateQuery = templateQuery;
    }

    private static Logger LOGGER = Logger.getLogger(YupanaPreparedStatement.class.getName());

    private String templateQuery;
    private Map<Integer, Proto.ParameterValue> parameters = new HashMap<>();
    private List<Map<Integer, Proto.ParameterValue>> batch = new ArrayList<>();

    private void setParameter(int idx, Proto.ParameterValue v) {
        parameters.put(idx, v);
    }

    private void setParameter(int idx, BigDecimal v) {
        setParameter(idx, Proto.ParameterValue.newBuilder()
                .setValue(Proto.Value.newBuilder()
                        .setDecimalValue(v.toString())).build());
    }

    @Override
    public void addBatch() throws SQLException {
        batch.add(parameters);
        parameters = new HashMap<>();
    }

    @Override
    public void clearParameters() throws SQLException {
        parameters = new HashMap<>();
    }

    @Override
    public void clearBatch() throws SQLException {
        batch.clear();
    }

    @Override
    public boolean execute() throws SQLException {
        LOGGER.log(Level.FINE, "Execute prepared statement {0}", templateQuery);
        Result result = connection.runQuery(templateQuery, parameters);
        lastResultSet = new YupanaResultSet(this, result);
        return true;
    }

    @Override
    public int[] executeBatch() throws SQLException {
        LOGGER.log(Level.FINE, "Execute prepared statement {0}", templateQuery);
        if (batch.isEmpty()) throw new SQLException("Batch is not defined");
        Result result = connection.runBatchQuery(templateQuery, batch);
        lastResultSet = new YupanaResultSet(this, result);
        int[] status = new int[batch.size()];
        Arrays.fill(status, 1);
        return status;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        execute();
        return lastResultSet;
    }

    @Override
    public int executeUpdate() throws SQLException {
        return 0;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }


    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setParameter(parameterIndex, BigDecimal.valueOf(x));
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setParameter(parameterIndex,BigDecimal.valueOf(x));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setParameter(parameterIndex, BigDecimal.valueOf(x));
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setParameter(parameterIndex, BigDecimal.valueOf(x));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setParameter(parameterIndex, BigDecimal.valueOf(x));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setParameter(parameterIndex, BigDecimal.valueOf(x));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setParameter(parameterIndex, Proto.ParameterValue.newBuilder().setValue(Proto.Value.newBuilder().setTextValue(x)).build());
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setParameter(parameterIndex, Proto.ParameterValue.newBuilder().setValue(Proto.Value.newBuilder().setTimeValue(x.getTime())).build());
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("SqlArrays are not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("InputStreams are not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("InputStreams are not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("InputStreams are not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("InputStreams are not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("InputStreams are not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("InputStreams are not supported");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blobs are not supported");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blobs are not supported");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blobs are not supported");
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Booleans are not supported");
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Bytes are not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams are not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams are not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams are not supported");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams are not supported");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams are not supported");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("InputStreams are not supported");
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clobs are not supported");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clobs are not supported");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clobs are not supported");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clobs are not supported");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clobs are not supported");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clobs are not supported");
    }

    @Override
    public void setDate(int parameterIndex, java.sql.Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Dates are not supported");
    }

    @Override
    public void setDate(int parameterIndex, java.sql.Date x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Dates are not supported");
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Time is not supported");
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Time is not supported");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Calendars are not supported");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("Objects are not supported");
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Objects are not supported");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("Objects are not supported");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("Nulls are not supported");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException("Nulls are not supported");
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Refs are not supported");
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException("URLs are not supported");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Row ids are not supported");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new SQLFeatureNotSupportedException("NStrings are not supported");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXMLs are not supported");
    }
}
