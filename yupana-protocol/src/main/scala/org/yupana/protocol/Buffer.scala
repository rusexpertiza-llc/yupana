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

trait Buffer[B] {
  def alloc(): B
  def alloc(capacity: Int): B
  def wrap(data: Array[Byte]): B
  def getBytes(b: B): Array[Byte]

  def readInt(b: B): Int
  def writeInt(b: B, i: Int): B

  def readDouble(b: B): Double
  def writeDouble(b: B, d: Double): B

  def readString(b: B, size: Int): String
  def writeString(b: B, s: String): Int

  def write(b: B, bytes: Array[Byte]): B
}

object Buffer {

  val DEFAULT_CAPACITY = 256

  implicit val nioBuffer: Buffer[ByteBuffer] = new Buffer[ByteBuffer] {
    override def alloc(): ByteBuffer = alloc(DEFAULT_CAPACITY)
    override def alloc(capacity: Int): ByteBuffer = ByteBuffer.allocate(capacity)
    override def wrap(data: Array[Byte]): ByteBuffer = ByteBuffer.wrap(data)
    override def getBytes(b: ByteBuffer): Array[Byte] = {
      b.array()
    }

    override def readInt(b: ByteBuffer): Int = b.getInt
    override def writeInt(b: ByteBuffer, i: Int): ByteBuffer = b.putInt(i)

    override def readDouble(b: ByteBuffer): Double = b.getDouble
    override def writeDouble(b: ByteBuffer, d: Double): ByteBuffer = b.putDouble(d)

    override def readString(b: ByteBuffer, size: Int): String = {
//      b.get
      ""
    }
    override def writeString(b: ByteBuffer, s: String): Int = {
      val bytes = s.getBytes(StandardCharsets.UTF_8)
      write(b, bytes)
      bytes.length
    }

    override def write(t: ByteBuffer, bytes: Array[Byte]): ByteBuffer = t.put(bytes)
  }
}
