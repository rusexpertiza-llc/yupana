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

import org.yupana.api.query.Result;

import java.sql.*;
import java.util.HashMap;

public class YupanaStatement implements Statement {

    public YupanaStatement(YupanaConnection connection) {
        this.connection = connection;
    }

    protected final YupanaConnection connection;

    private int maxRows = 0;
    private int fetchSize = 0;
    protected YupanaResultSet lastResultSet = null;

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        execute(sql);
        return lastResultSet;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        Result result = connection.runQuery(sql, new HashMap<>());
        lastResultSet = new YupanaResultSet(this, result);
        return true;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported: Statement.executeUpdate(String,int)");
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported: getMaxFieldSize()");
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported: setMaxFieldSize(int)");
    }

    @Override
    public int getMaxRows() throws SQLException {
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported: setEscapeProcessing(boolean)");
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported: setQueryTimeout(int)");
    }

    @Override
    public void cancel() throws SQLException {
        close();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported: setCursorName(String)");
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return lastResultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return -1;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int fetchDirection) throws SQLException {
        if (fetchDirection != ResultSet.FETCH_FORWARD)
            throw new SQLException(String.format("Unsupported fetch direction %d", fetchDirection));
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported: Statement.addBatch(String)");
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported: Statement.clearBatch()");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported: Statement.executeBatch()");
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported: Statement.getMoreResults(int)");
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported: Statement.getGeneratedKeys()");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported: Statement.executeUpdate(String,int)");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported: : Statement.executeUpdate(String,int[])");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported: Statement.executeUpdate(String,String[])");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported: Statement.execute(String,int)");
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported: Statement.execute(String,int[])");
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported: Statement.execute(String,String[])");
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return connection.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throw new SQLFeatureNotSupportedException("Pooling is not supported");
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported: Statement.closeOnCompletion()");
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
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
