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
import org.yupana.api.{ Blob, Currency, Time }

import scala.reflect.ClassTag

trait ReaderWriter[B, V[_], S, O] {

  def sizeOfBoolean: S

  def readBoolean(b: B): V[Boolean]
  def readBoolean(b: B, offset: O): V[Boolean]

  def writeBoolean(b: B, v: V[Boolean]): S
  def writeBoolean(b: B, offset: O, v: V[Boolean]): S

  def sizeOfByte: S

  def readBytes(b: B, d: V[Array[Byte]]): V[Unit]
  def readBytes(b: B, offset: O, d: V[Array[Byte]]): V[Unit]

  def writeBytes(b: B, v: V[Array[Byte]]): S
  def writeBytes(b: B, offset: O, v: V[Array[Byte]]): S

  def sizeOfInt: S

  def readInt(b: B): V[Int]
  def readInt(b: B, offset: O): V[Int]
  def writeInt(b: B, v: V[Int]): S
  def writeInt(b: B, offset: O, v: V[Int]): S

  def sizeOfLong: S

  def readLong(b: B): V[Long]
  def readLong(b: B, offset: O): V[Long]

  def writeLong(b: B, v: V[Long]): S
  def writeLong(b: B, offset: O, v: V[Long]): S

  def sizeOfDouble: S

  def readDouble(b: B): V[Double]
  def readDouble(b: B, offset: O): V[Double]
  def writeDouble(b: B, v: V[Double]): S
  def writeDouble(b: B, offset: O, v: V[Double]): S

  def sizeOfShort: S

  def readShort(b: B): V[Short]
  def readShort(b: B, offset: O): V[Short]
  def writeShort(b: B, v: V[Short]): S
  def writeShort(b: B, offset: O, v: V[Short]): S

  def readByte(b: B): V[Byte]
  def readByte(b: B, offset: O): V[Byte]

  def writeByte(b: B, v: V[Byte]): S
  def writeByte(b: B, offset: O, v: V[Byte]): S

  def sizeOfString(v: V[String]): S

  def readString(b: B): V[String]
  def readString(b: B, offset: O): V[String]

  def writeString(b: B, v: V[String]): S
  def writeString(b: B, offset: O, v: V[String]): S

  def readVLong(b: B): V[Long]
  def readVLong(b: B, offset: O): V[Long]

  def writeVLong(b: B, v: V[Long]): S
  def writeVLong(b: B, offset: O, v: V[Long]): S

  def readVInt(b: B): V[Int]
  def readVInt(b: B, offset: O): V[Int]

  def writeVInt(b: B, v: V[Int]): S
  def writeVInt(b: B, offset: O, v: V[Int]): S

  def readVShort(b: B): V[Short]
  def readVShort(b: B, offset: O): V[Short]

  def writeVShort(b: B, v: V[Short]): S
  def writeVShort(b: B, offset: O, v: V[Short]): S

  def sizeOfBigDecimal(v: V[BigDecimal]): S

  def readBigDecimal(b: B): V[BigDecimal]
  def readBigDecimal(b: B, offset: O): V[BigDecimal]

  def writeBigDecimal(b: B, v: V[BigDecimal]): S
  def writeBigDecimal(b: B, offset: O, v: V[BigDecimal]): S

  def sizeOfTime: S

  def readTime(b: B): V[Time]
  def readTime(b: B, offset: O): V[Time]

  def writeTime(b: B, v: V[Time]): S
  def writeTime(b: B, offset: O, v: V[Time]): S

  def readVTime(b: B): V[Time]
  def readVTime(b: B, offset: O): V[Time]

  def writeVTime(b: B, v: V[Time]): S
  def writeVTime(b: B, offset: O, v: V[Time]): S

  def sizeOfCurrency: S

  def readCurrency(b: B): V[Currency]
  def readCurrency(b: B, offset: O): V[Currency]

  def writeCurrency(b: B, v: V[Currency]): S
  def writeCurrency(b: B, offset: O, v: V[Currency]): S

  def readVCurrency(b: B): V[Currency]
  def readVCurrency(b: B, offset: O): V[Currency]

  def writeVCurrency(b: B, v: V[Currency]): S
  def writeVCurrency(b: B, offset: O, v: V[Currency]): S

  def sizeOfTuple[T, U](v: V[(T, U)], tSize: V[T] => S, uSize: V[U] => S): S

  def readTuple[T, U](b: B, tReader: B => V[T], uReader: B => V[U]): V[(T, U)]
  def readTuple[T, U](b: B, offset: O, tReader: B => V[T], uReader: B => V[U]): V[(T, U)]

  def writeTuple[T, U](bb: B, v: V[(T, U)], tWrite: (B, V[T]) => S, uWrite: (B, V[U]) => S): S

  def writeTuple[T, U](
      bb: B,
      offset: O,
      v: V[(T, U)],
      tWrite: (B, V[T]) => S,
      uWrite: (B, V[U]) => S
  ): S

  def sizeOfSeq[T](v: V[Seq[T]], size: V[T] => S): S

  def readSeq[T: ClassTag](b: B, reader: B => V[T]): V[Seq[T]]
  def readSeq[T: ClassTag](b: B, offset: O, reader: B => V[T]): V[Seq[T]]

  def writeSeq[T](b: B, seq: V[Seq[T]], writer: (B, V[T]) => S)(implicit ct: ClassTag[T]): S
  def writeSeq[T](b: B, offset: O, seq: V[Seq[T]], writer: (B, V[T]) => S)(implicit ct: ClassTag[T]): S

  def sizeOfBlob(v: V[Blob]): S

  def readBlob(b: B): V[Blob]
  def readBlob(b: B, offset: O): V[Blob]

  def writeBlob(b: B, v: V[Blob]): S
  def writeBlob(b: B, offset: O, v: V[Blob]): S

  def sizeOfPeriodDuration(v: V[PeriodDuration]): S

  def readPeriodDuration(b: B): V[PeriodDuration]
  def readPeriodDuration(b: B, offset: O): V[PeriodDuration]

  def writePeriodDuration(b: B, v: V[PeriodDuration]): S
  def writePeriodDuration(b: B, offset: O, v: V[PeriodDuration]): S
}
