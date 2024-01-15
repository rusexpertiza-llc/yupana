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

package org.yupana.readerwriter

import jdk.internal.foreign.AbstractMemorySegmentImpl
import jdk.internal.misc.Unsafe
import org.yupana.readerwriter.MemoryBuffer.convertEndian

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.lang.foreign.{Arena, MemorySegment}
import scala.util.hashing.MurmurHash3

final class MemoryBuffer(private var base: AnyRef, private var baseOffset: Long, private var initSize: Long) extends Serializable {

  import org.yupana.readerwriter.MemoryBuffer.UNSAFE

  private var pos: Int = 0

  def size: Long = {
    this.initSize
  }
  def position(): Int = {
    pos
  }

  def position(newPos: Int): Unit = {
    pos = newPos
  }

  def hasRemaining(): Boolean = {
    pos < size
  }

  def rewind(): Unit = {
    pos = 0
  }

  def asSlice(offset: Int, size: Long): MemoryBuffer = {
    new MemoryBuffer(base, baseOffset + offset, size)
  }

  def asSlice(offset: Int): MemoryBuffer = {
    new MemoryBuffer(base, baseOffset + offset, size - offset)
  }

  def get(bytes: Array[Byte]): Unit = {
    val p = pos
    pos += bytes.length
    UNSAFE.copyMemory(
      base,
      baseOffset + p,
      bytes,
      Unsafe.ARRAY_BYTE_BASE_OFFSET,
      bytes.length
    )
  }

  def get(offset: Int, bytes: Array[Byte]): Unit = {
    UNSAFE.copyMemory(
      base,
      baseOffset + offset,
      bytes,
      Unsafe.ARRAY_BYTE_BASE_OFFSET,
      bytes.length
    )
  }

  def put(bytes: Array[Byte]): Unit = {
    val p = pos
    pos += bytes.length
    UNSAFE.copyMemory(
      bytes,
      Unsafe.ARRAY_BYTE_BASE_OFFSET,
      base,
      baseOffset + p,
      bytes.length
    )
  }

  def put(offset: Int, bytes: Array[Byte]): Unit = {
    UNSAFE.copyMemory(
      bytes,
      Unsafe.ARRAY_BYTE_BASE_OFFSET,
      base,
      baseOffset + offset,
      bytes.length
    )
  }

  def put(offset: Int, src: MemoryBuffer, srcOffset: Int, size: Int): Unit = {
    UNSAFE.copyMemory(
      src.base,
      src.baseOffset + srcOffset,
      base,
      baseOffset + offset,
      size
    )
  }

  def get(): Byte = {
    val p = pos
    pos += 1
    UNSAFE.getByte(base, baseOffset + p)
  }

  def get(offset: Int): Byte = {
    UNSAFE.getByte(base, baseOffset + offset)
  }

  def put(v: Byte): Unit = {
    val p = pos
    pos += 1
    UNSAFE.putByte(base, baseOffset + p, v)
  }

  def put(offset: Int, v: Byte): Unit = {
    UNSAFE.putByte(base, baseOffset + offset, v)
  }

  def getInt(): Int = {
    val p = pos
    pos += 4
    convertEndian(UNSAFE.getInt(base, baseOffset + p))
  }

  def getInt(offset: Int): Int = {
    convertEndian(UNSAFE.getInt(base, baseOffset + offset))
  }

  def putInt(v: Int): Unit = {
    val p = pos
    pos += 4
    UNSAFE.putInt(base, baseOffset + p, convertEndian(v))
  }

  def putInt(offset: Int, v: Int): Unit = {
    UNSAFE.putInt(base, baseOffset + offset, convertEndian(v))
  }

  def getLong(): Long = {
    val p = pos
    pos += 8
    convertEndian(UNSAFE.getLong(base, baseOffset + p))
  }

  def getLong(offset: Int): Long = {
    convertEndian(UNSAFE.getLong(base, baseOffset + offset))
  }

  def putLong(v: Long): Any = {
    val p = pos
    pos += 8
    UNSAFE.putLong(base, baseOffset + p, convertEndian(v))
  }

  def putLong(offset: Int, v: Long): Unit = {
    UNSAFE.putLong(base, baseOffset + offset, convertEndian(v))
  }

  def getDouble(): Double = {
    val p = pos
    pos += 8
    val b = convertEndian(UNSAFE.getLong(base, baseOffset + p))
    java.lang.Double.longBitsToDouble(b)
  }

  def getDouble(offset: Int): Double = {
    val b = convertEndian(UNSAFE.getLong(base, baseOffset + offset))
    java.lang.Double.longBitsToDouble(b)
  }

  def putDouble(v: Double): Unit = {
    val p = pos
    pos += 8
    val b = convertEndian(java.lang.Double.doubleToRawLongBits(v))
    UNSAFE.putLong(base, baseOffset + p, b)
  }

  def putDouble(offset: Int, v: Double): Unit = {
    val b = convertEndian(java.lang.Double.doubleToRawLongBits(v))
    UNSAFE.putLong(base, baseOffset + offset, b)
  }

  def getShort(): Short = {
    val p = pos
    pos += 2
    convertEndian(UNSAFE.getShort(base, baseOffset + p))
  }

  def getShort(offset: Int): Short = {
    convertEndian(UNSAFE.getShort(base, baseOffset + offset))
  }

  def putShort(v: Short): Unit = {
    val p = pos
    pos += 2
    UNSAFE.putShort(base, baseOffset + p, convertEndian(v))
  }

  def putShort(offset: Int, v: Short): Unit = {
    UNSAFE.putShort(base, baseOffset + offset, convertEndian(v))
  }

  override def hashCode(): Int = {
    var h = MurmurHash3.arraySeed
    var i = 0
    while (i < size) {
      h = MurmurHash3.mix(h, get(i))
      i += 1
    }
    MurmurHash3.finalizeHash(h, size.toInt)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: MemoryBuffer if that.size == size =>
        var i = 0
        while (i < size && get(i) == that.get(i)) {
          i += 1
        }
        i == size
      case _ => false
    }
  }

  def bytes(): Array[Byte] = {
    val bs = Array.ofDim[Byte](size.toInt)
    get(bs)
    bs
  }


  // Overriding Java serialization
  private def writeObject(oos: ObjectOutputStream): Unit = {
    oos.writeInt(size.toInt)
    oos.write(bytes())
  }

  private def readObject(ois: ObjectInputStream): Unit = {
    val s = ois.readInt()
    this.initSize = s
    val array = Array.ofDim[Byte](s)
    ois.readFully(array)
    this.base = array
    this.baseOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET
  }
}

object MemoryBuffer {

  private val UNSAFE = Unsafe.getUnsafe

  def allocateHeap(size: Int): MemoryBuffer = {
    val a = UNSAFE.allocateUninitializedArray(classOf[Byte], size)
    val baseOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET
    new MemoryBuffer(a, baseOffset, size)
  }

  def allocateNative(size: Int): MemoryBuffer = {
    val seg = Arena
      .ofConfined()
      .allocate(size, 8)
      .asInstanceOf[AbstractMemorySegmentImpl]
    val base = seg.unsafeGetBase()
    val baseOffset = seg.unsafeGetOffset()
    new MemoryBuffer(base, baseOffset, size)
  }

  def ofBytes(bytes: Array[Byte]): MemoryBuffer = {
    val baseOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET
    new MemoryBuffer(bytes, baseOffset, bytes.length)
  }

  def ofMemorySegment(segment: MemorySegment): MemoryBuffer = {
    val seg = segment.asInstanceOf[AbstractMemorySegmentImpl]
    val base = seg.unsafeGetBase()
    val baseOffset = seg.unsafeGetOffset()
    val size = seg.byteSize()
    new MemoryBuffer(base, baseOffset, size)
  }

  private def convertEndian(v: Int): Int = {
    java.lang.Integer.reverseBytes(v)
  }

  private def convertEndian(v: Long): Long = {
    java.lang.Long.reverseBytes(v)
  }

  private def convertEndian(v: Short): Short = {
    java.lang.Short.reverseBytes(v)
  }
}
