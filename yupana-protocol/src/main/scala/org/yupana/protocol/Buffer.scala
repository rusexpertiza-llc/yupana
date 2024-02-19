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

package org.yupana.protocol

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/**
  * Type class to abstract from different buffer implementations, for example [[java.nio.ByteBuffer]] and Netty ByteBuf.
  * @tparam B buffer type
  */
trait Buffer[B] {
  def alloc(): B
  def alloc(capacity: Int): B
  def wrap(data: Array[Byte]): B
  def toByteArray(b: B): Array[Byte]

  def readByte(b: B): Byte
  def writeByte(b: B, v: Byte): B

  def readInt(b: B): Int
  def writeInt(b: B, i: Int): B

  def readLong(b: B): Long
  def writeLong(b: B, l: Long): B

  def readDouble(b: B): Double
  def writeDouble(b: B, d: Double): B

  def readString(b: B): String = {
    val length = readInt(b)
    val bytes = new Array[Byte](length)
    read(b, bytes)
    new String(bytes, StandardCharsets.UTF_8)
  }

  def writeString(b: B, s: String): Int = {
    val bytes = s.getBytes(StandardCharsets.UTF_8)
    writeInt(b, bytes.length)
    write(b, bytes)
    bytes.length
  }

  def read(b: B, dst: Array[Byte]): Unit
  def write(b: B, bytes: Array[Byte]): B
}

object Buffer {

  val DEFAULT_CAPACITY: Int = 256

  implicit val nioBuffer: Buffer[ByteBuffer] = new Buffer[ByteBuffer] {
    override def alloc(): ByteBuffer = alloc(DEFAULT_CAPACITY)
    override def alloc(capacity: Int): ByteBuffer = ByteBuffer.allocate(capacity)
    override def wrap(data: Array[Byte]): ByteBuffer = ByteBuffer.wrap(data)
    override def toByteArray(b: ByteBuffer): Array[Byte] = {
      val res = new Array[Byte](b.position())
      b.flip()
      b.get(res)
      res
    }

    override def readByte(b: ByteBuffer): Byte = b.get
    override def writeByte(b: ByteBuffer, v: Byte): ByteBuffer = b.put(v)

    override def readInt(b: ByteBuffer): Int = b.getInt
    override def writeInt(b: ByteBuffer, i: Int): ByteBuffer = b.putInt(i)

    override def readLong(b: ByteBuffer): Long = b.getLong()
    override def writeLong(b: ByteBuffer, l: Long): ByteBuffer = b.putLong(l)

    override def readDouble(b: ByteBuffer): Double = b.getDouble
    override def writeDouble(b: ByteBuffer, d: Double): ByteBuffer = b.putDouble(d)

    override def read(b: ByteBuffer, dst: Array[Byte]): Unit = b.get(dst)
    override def write(t: ByteBuffer, bytes: Array[Byte]): ByteBuffer = t.put(bytes)
  }
}
