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

package org.yupana.api.types

import org.threeten.extra.PeriodDuration
import org.yupana.api.{ Blob, Time }

import scala.reflect.ClassTag

trait ReaderWriter[B, V[_], WR[_]] {

  def readBoolean(b: B): V[Boolean]
  def readBoolean(b: B, offset: Int): V[Boolean]

  def writeBoolean(b: B, v: V[Boolean]): WR[Boolean]
  def writeBoolean(b: B, offset: Int, v: V[Boolean]): WR[Boolean]

  def readInt(b: B): V[Int]
  def readInt(b: B, offset: Int): V[Int]
  def writeInt(b: B, v: V[Int]): WR[Int]
  def writeInt(b: B, offset: Int, v: V[Int]): WR[Int]

  def readLong(b: B): V[Long]
  def readLong(b: B, offset: Int): V[Long]

  def writeLong(b: B, v: V[Long]): WR[Long]
  def writeLong(b: B, offset: Int, v: V[Long]): WR[Long]

  def readDouble(b: B): V[Double]
  def readDouble(b: B, offset: Int): V[Double]
  def writeDouble(b: B, v: V[Double]): WR[Double]
  def writeDouble(b: B, offset: Int, v: V[Double]): WR[Double]

  def readShort(b: B): V[Short]
  def readShort(b: B, offset: Int): V[Short]
  def writeShort(b: B, v: V[Short]): WR[Short]
  def writeShort(b: B, offset: Int, v: V[Short]): WR[Short]

  def readByte(b: B): V[Byte]
  def readByte(b: B, offset: Int): V[Byte]

  def writeByte(b: B, v: V[Byte]): WR[Byte]
  def writeByte(b: B, offset: Int, v: V[Byte]): WR[Byte]

  def readString(b: B): V[String]
  def readString(b: B, offset: Int): V[String]

  def writeString(b: B, v: V[String]): WR[String]
  def writeString(b: B, offset: Int, v: V[String]): WR[String]

  def readVLong(b: B): V[Long]
  def readVLong(b: B, offset: Int): V[Long]

  def writeVLong(b: B, v: V[Long]): WR[Long]
  def writeVLong(b: B, offset: Int, v: V[Long]): WR[Long]

  def readVInt(b: B): V[Int]
  def readVInt(b: B, offset: Int): V[Int]

  def writeVInt(b: B, v: V[Int]): WR[Int]
  def writeVInt(b: B, offset: Int, v: V[Int]): WR[Int]

  def readVShort(b: B): V[Short]
  def readVShort(b: B, offset: Int): V[Short]

  def writeVShort(b: B, v: V[Short]): WR[Short]
  def writeVShort(b: B, offset: Int, v: V[Short]): WR[Short]
  def readBigDecimal(b: B): V[BigDecimal]
  def readBigDecimal(b: B, offset: Int): V[BigDecimal]

  def writeBigDecimal(b: B, v: V[BigDecimal]): WR[BigDecimal]
  def writeBigDecimal(b: B, offset: Int, v: V[BigDecimal]): WR[BigDecimal]

  def readTime(b: B): V[Time]
  def readTime(b: B, offset: Int): V[Time]

  def writeTime(b: B, v: V[Time]): WR[Time]
  def writeTime(b: B, offset: Int, v: V[Time]): WR[Time]

  def readVTime(b: B): V[Time]
  def readVTime(b: B, offset: Int): V[Time]

  def writeVTime(b: B, v: V[Time]): WR[Time]
  def writeVTime(b: B, offset: Int, v: V[Time]): WR[Time]

  def readTuple[T, U](b: B, tReader: (B) => V[T], uReader: (B) => V[U]): V[(T, U)]
  def readTuple[T, U](b: B, offset: Int, tReader: (B) => V[T], uReader: (B) => V[U]): V[(T, U)]

  def writeTuple[T, U](bb: B, v: V[(T, U)], tWrite: (B, V[T]) => WR[T], uWrite: (B, V[U]) => WR[U]): WR[(T, U)]

  def writeTuple[T, U](
      bb: B,
      offset: Int,
      v: V[(T, U)],
      tWrite: (B, V[T]) => WR[T],
      uWrite: (B, V[U]) => WR[U]
  ): WR[(T, U)]
  def readSeq[T: ClassTag](b: B, reader: B => V[T]): V[Seq[T]]
  def readSeq[T: ClassTag](b: B, offset: Int, reader: B => V[T]): V[Seq[T]]

  def writeSeq[T](b: B, seq: V[Seq[T]], writer: (B, V[T]) => WR[T])(implicit ct: ClassTag[T]): WR[Seq[T]]
  def writeSeq[T](b: B, offset: Int, seq: V[Seq[T]], writer: (B, V[T]) => WR[T])(implicit ct: ClassTag[T]): WR[Seq[T]]

  def readBlob(b: B): V[Blob]
  def readBlob(b: B, offset: Int): V[Blob]

  def writeBlob(b: B, v: V[Blob]): WR[Blob]
  def writeBlob(b: B, offset: Int, v: V[Blob]): WR[Blob]

  def readPeriodDuration(b: B): V[PeriodDuration]
  def readPeriodDuration(b: B, offset: Int): V[PeriodDuration]

  def writePeriodDuration(b: B, v: V[PeriodDuration]): WR[PeriodDuration]
  def writePeriodDuration(b: B, offset: Int, v: V[PeriodDuration]): WR[PeriodDuration]
}
