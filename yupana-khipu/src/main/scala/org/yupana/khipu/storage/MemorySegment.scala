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

package org.yupana.khipu.storage

import jdk.internal.misc.Unsafe
import org.yupana.readerwriter.Memory._
import MemorySegment._
import jdk.incubator.foreign
import jdk.internal.foreign.AbstractMemorySegmentImpl
import org.yupana.readerwriter.MemoryBuffer

import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.{ Path, StandardOpenOption }
final class MemorySegment(private val base: AnyRef, private val baseOffset: Long, private val initSize: Long) {

  def asMemoryBuffer(): MemoryBuffer = {
    new MemoryBuffer(base, baseOffset, initSize)
  }

  def byteSize(): Long = initSize

  def asSlice(offset: Long): MemorySegment = {
    asSlice(offset, byteSize() - offset)
  }

  def asSlice(offset: Long, size: Long): MemorySegment = {
    new MemorySegment(base, baseOffset + offset, size)
  }

  def toArray: Array[Byte] = {
    val s = byteSize().toInt
    val a = Array.ofDim[Byte](s)
    MemorySegment.copy(this, 0, a, 0, s)
    a
  }

  def getInt(offset: Long): Int = {
    convertEndian(UNSAFE.getInt(base, baseOffset + offset))
  }

  def getIntUnaligned(offset: Long): Int = {
    getInt(offset)
  }

  def setInt(offset: Long, v: Int): Unit = {
    UNSAFE.putInt(base, baseOffset + offset, convertEndian(v))
  }

  def setIntUnaligned(offset: Long, v: Int): Unit = {
    setInt(offset, v)
  }

  def getLong(offset: Long): Long = {
    convertEndian(UNSAFE.getLong(base, baseOffset + offset))
  }

  def getLongUnaligned(offset: Long): Long = {
    getLong(offset)
  }

  def setLong(offset: Long, v: Long): Unit = {
    UNSAFE.putLong(base, baseOffset + offset, convertEndian(v))
  }

  def setLongUnaligned(offset: Long, v: Long): Unit = {
    setLong(offset, v)
  }

  def getByte(offset: Long): Byte = {
    UNSAFE.getByte(base, baseOffset + offset)
  }

  def setByte(offset: Long, v: Byte): Unit = {
    UNSAFE.putByte(base, baseOffset + offset, v)
  }

  def getShort(offset: Long): Short = {
    convertEndian(UNSAFE.getShort(base, baseOffset + offset))

  }

  def setShort(offset: Long, v: Short): Unit = {
    UNSAFE.putShort(base, baseOffset + offset, convertEndian(v))
  }

  def setDouble(offset: Long, v: Double): Unit = {
    val l = convertEndian(java.lang.Double.doubleToRawLongBits(v))
    UNSAFE.putLong(base, baseOffset + offset, l)
  }

}

object MemorySegment {

  private val UNSAFE = Unsafe.getUnsafe

  def ofArray(a: Array[Byte]): MemorySegment = {
    new MemorySegment(a, Unsafe.ARRAY_BYTE_BASE_OFFSET, a.length)
  }

//  def ofArray(a: Array[Long]): MemorySegment = {
//    new MemorySegment(a, Unsafe.ARRAY_LONG_BASE_OFFSET, a.length)
//  }

  def allocateNative(size: Int): MemorySegment = {
    val baseOffset = UNSAFE.allocateMemory(size)
    new MemorySegment(null, baseOffset, size)
  }

  def mapFile(path: Path, offset: Long, size: Int): MemorySegment = {
    val ch = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)
    val bf = ch.map(MapMode.READ_WRITE, offset, size)
    val fseg = foreign.MemorySegment.ofByteBuffer(bf).asInstanceOf[AbstractMemorySegmentImpl]
    new MemorySegment(fseg.unsafeGetBase(), fseg.unsafeGetOffset(), size)
  }
  def copy(src: MemorySegment, srcOffset: Long, dst: Array[Byte], index: Int, size: Int): Unit = {
    UNSAFE.copyMemory(
      src.base,
      src.baseOffset + srcOffset,
      dst,
      Unsafe.ARRAY_BYTE_BASE_OFFSET + index,
      size
    )
  }

  def copy(src: Array[Byte], index: Int, dst: MemorySegment, dstOffset: Long, size: Int): Unit = {
    UNSAFE.copyMemory(
      src,
      Unsafe.ARRAY_BYTE_BASE_OFFSET + index,
      dst.base,
      dst.baseOffset + dstOffset,
      size
    )
  }

  def copy(src: MemorySegment, srcOffset: Long, dst: MemorySegment, dstOffset: Long, size: Long): Unit = {
    UNSAFE.copyMemory(
      src.base,
      src.baseOffset + srcOffset,
      dst.base,
      dst.baseOffset + dstOffset,
      size
    )
  }
}
