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

import org.yupana.api.Blob

import scala.reflect.ClassTag

trait SizeSpecifiedReaderWriter[B, V[_], S, O] {

  def sizeOfString2(v: V[String]): S

  def readString2(b: B, size: S): V[String]
  def readString2(b: B, offset: O, size: S): V[String]

  def writeString2(b: B, v: V[String]): S
  def writeString2(b: B, offset: O, v: V[String]): S

  def sizeOfBigDecimal2(v: V[BigDecimal]): S

  def readBigDecimal2(b: B, size: S): V[BigDecimal]
  def readBigDecimal2(b: B, offset: O, size: S): V[BigDecimal]

  def writeBigDecimal2(b: B, v: V[BigDecimal]): S
  def writeBigDecimal2(b: B, offset: O, v: V[BigDecimal]): S

  def sizeOfTuple2[T, U](v: V[(T, U)], tSize: V[T] => S, uSize: V[U] => S): S

  def readTuple2[T, U](b: B, tReader: (B, S) => V[T], uReader: (B, S) => V[U]): V[(T, U)]

  def readTuple2[T, U](b: B, offset: O, tReader: (B, S) => V[T], uReader: (B, S) => V[U]): V[(T, U)]

  def writeTuple2[T, U](bb: B, v: V[(T, U)], tWrite: (B, V[T]) => S, uWrite: (B, V[U]) => S): S

  def writeTuple2[T, U](
      bb: B,
      offset: O,
      v: V[(T, U)],
      tWrite: (B, V[T]) => S,
      uWrite: (B, V[U]) => S
  ): S

  def sizeOfSeq2[T](v: V[Seq[T]], size: V[T] => S): S
  def readSeq2[T: ClassTag](b: B, reader: (B, S) => V[T]): V[Seq[T]]
  def readSeq2[T: ClassTag](b: B, offset: O, reader: (B, S) => V[T]): V[Seq[T]]

  def writeSeq2[T](b: B, seq: V[Seq[T]], writer: (B, V[T]) => S)(implicit ct: ClassTag[T]): S
  def writeSeq2[T](b: B, offset: O, seq: V[Seq[T]], writer: (B, V[T]) => S)(implicit ct: ClassTag[T]): S

  def sizeOfBlob2(v: V[Blob]): S

  def readBlob2(b: B, size: S): V[Blob]
  def readBlob2(b: B, offset: O, size: S): V[Blob]

  def writeBlob2(b: B, v: V[Blob]): S
  def writeBlob2(b: B, offset: O, v: V[Blob]): S

}
