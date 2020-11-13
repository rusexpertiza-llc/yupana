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

import java.io.{ ByteArrayInputStream, InputStream, OutputStream }
import java.sql.{ Blob, SQLException, SQLFeatureNotSupportedException }

class YupanaBlob(data: Array[Byte]) extends Blob {
  override def length(): Long = data.length

  override def getBytes(pos: Long, length: Int): Array[Byte] = {
    if (pos < 1 || pos > Int.MaxValue) throw new SQLException(s"Position must be >= 1 and < ${Int.MaxValue}")
    if (length < 0) throw new SQLException("Length must be >= 0")
    data.slice((pos - 1).toInt, (pos - 1 + length).toInt)
  }

  override def getBinaryStream: InputStream = new ByteArrayInputStream(data)

  override def getBinaryStream(pos: Long, length: Long): InputStream =
    new ByteArrayInputStream(getBytes(pos, length.toInt))

  override def position(pattern: Array[Byte], start: Long): Long =
    throw new SQLFeatureNotSupportedException("Data searching in BLOBs is not supported")

  override def position(pattern: Blob, start: Long): Long =
    throw new SQLFeatureNotSupportedException("Data searching in BLOBs is not supported")

  override def setBytes(pos: Long, bytes: Array[Byte]): Int =
    throw new SQLFeatureNotSupportedException("Yupana BLOB is immutable")

  override def setBytes(pos: Long, bytes: Array[Byte], offset: Int, len: Int): Int =
    throw new SQLFeatureNotSupportedException("Yupana BLOB is immutable")

  override def setBinaryStream(pos: Long): OutputStream =
    throw new SQLFeatureNotSupportedException("Yupana BLOB is immutable")

  override def truncate(len: Long): Unit =
    throw new SQLFeatureNotSupportedException("Yupana BLOB is immutable")

  override def free(): Unit = {}
}
