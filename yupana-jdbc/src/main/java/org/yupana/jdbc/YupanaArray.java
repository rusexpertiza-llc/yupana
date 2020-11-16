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

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;

import org.yupana.api.types.DataTypeMeta;
import scala.NotImplementedError;

class YupanaArray<T> implements Array {

    private final String name;
    private final T[] values;
    private final DataTypeMeta<T> valueType;

    public YupanaArray(String name, T[] values, DataTypeMeta<T> valueType) {
        this.name = name;
        this.values = values;
        this.valueType = valueType;
    }


    @Override
    public String getBaseTypeName() throws SQLException {
        return valueType.sqlTypeName();
    }

    @Override
    public int getBaseType() throws SQLException {
        return valueType.sqlType();
    }

    @Override
    public T[] getArray() throws SQLException {
        return values;
    }

    @Override
    public T[] getArray(Map<String, Class<?>> map) throws SQLException {
        JdbcUtils.checkTypeMapping(map);
        return getArray();
    }

    @Override
    public T[] getArray(long index, int count) throws SQLException {
        int start = (int) (index - 1);
        return Arrays.copyOfRange(values, start, start + count);
    }

    @Override
    public T[] getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        JdbcUtils.checkTypeMapping(map);
        return getArray(index, count);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return createResultSet(values, 1);
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        JdbcUtils.checkTypeMapping(map);
        return getResultSet();
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        return createResultSet(getArray(index, count), (int) index);
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        JdbcUtils.checkTypeMapping(map);
        return getResultSet(index, count);
    }

    @Override
    public void free() throws SQLException {

    }

    private ResultSet createResultSet(T[] array, int startIndex) {
        throw new NotImplementedError("Not implemented yet");
//    val it = array.zip(Stream.from(startIndex)).map { case (v, i) => Array[Any](i, v) }.toIterator
//    new YupanaResultSet(null, new SimpleResult(name, Seq("INDEX", "VALUE"), Seq(DataType[Int], valueType), it))
    }
}
