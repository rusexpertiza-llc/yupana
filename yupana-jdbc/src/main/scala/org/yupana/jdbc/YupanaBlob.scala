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
import java.nio.ByteBuffer
import java.sql.{ Blob, SQLFeatureNotSupportedException }

class YupanaBlob(bytes: Array[Byte]) extends Blob {

  private var data = bytes

  override def length(): Long = data.length

  override def getBytes(pos: Long, length: Int): Array[Byte] = {
    if (pos < data.length) {
      ByteBuffer.wrap(bytes, pos.toInt, length).array()
    } else Array.empty
  }

  override def getBinaryStream: InputStream = {
    new ByteArrayInputStream(bytes)
  }

  override def position(pattern: Array[Byte], start: Long): Long = noSearching

  override def position(pattern: Blob, start: Long): Long = noSearching

  override def setBytes(pos: Long, bytes: Array[Byte]): Int = readOnlyError

  override def setBytes(pos: Long, bytes: Array[Byte], offset: Int, len: Int): Int = readOnlyError

  override def setBinaryStream(pos: Long): OutputStream = readOnlyError

  override def truncate(len: Long): Unit = readOnlyError

  override def free(): Unit = data = Array.empty[Byte]

  override def getBinaryStream(pos: Long, length: Long): InputStream = readOnlyError

  private def readOnlyError: Nothing = throw new SQLFeatureNotSupportedException("Yupana BLOB is read only")

  private def noSearching: Nothing = throw new SQLFeatureNotSupportedException("Searching is not supported")
}
