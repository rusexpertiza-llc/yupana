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


import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;

public class YupanaBlob implements Blob {

    private final byte[] data;

    public YupanaBlob(byte[] data) {
        this.data = data;
    }

    @Override
    public long length() throws SQLException {
        return data.length;
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        if (pos < 1 || pos > Integer.MAX_VALUE)
            throw new SQLException(String.format("Position must be >= 1 and < %d", Integer.MAX_VALUE));
        if (length < 0) throw new SQLException("Length must be >= 0");
        return Arrays.copyOfRange(data, (int) (pos - 1), (int) (pos - 1 + length));

    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        return new ByteArrayInputStream(data);
    }

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        return new ByteArrayInputStream(getBytes(pos, (int) length));
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
        throw new SQLFeatureNotSupportedException("Data searching in BLOBs is not supported");
    }

    @Override
    public long position(Blob pattern, long start) throws SQLException {
        throw new SQLFeatureNotSupportedException("Data searching in BLOBs is not supported");
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Yupana BLOB is immutable");
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        throw new SQLFeatureNotSupportedException("Yupana BLOB is immutable");
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        throw new SQLFeatureNotSupportedException("Yupana BLOB is immutable");
    }

    @Override
    public void truncate(long len) throws SQLException {
        throw new SQLFeatureNotSupportedException("Yupana BLOB is immutable");
    }

    @Override
    public void free() throws SQLException {

    }
}
