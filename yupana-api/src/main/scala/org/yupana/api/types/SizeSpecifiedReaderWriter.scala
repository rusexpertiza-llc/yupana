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

  def sizeOfStringSizeSpecified(v: V[String]): S

  def readStringSizeSpecified(b: B, size: S): V[String]
  def readStringSizeSpecified(b: B, offset: O, size: S): V[String]

  def writeStringSizeSpecified(b: B, v: V[String]): S
  def writeStringSizeSpecified(b: B, offset: O, v: V[String]): S

  def sizeOfBigDecimalSizeSpecified(v: V[BigDecimal]): S

  def readBigDecimalSizeSpecified(b: B, size: S): V[BigDecimal]
  def readBigDecimalSizeSpecified(b: B, offset: O, size: S): V[BigDecimal]

  def writeBigDecimalSizeSpecified(b: B, v: V[BigDecimal]): S
  def writeBigDecimalSizeSpecified(b: B, offset: O, v: V[BigDecimal]): S

  def sizeOfTupleSizeSpecified[T, U](v: V[(T, U)], tSize: V[T] => S, uSize: V[U] => S): S

  def readTupleSizeSpecified[T, U](b: B, tReader: (B, S) => V[T], uReader: (B, S) => V[U]): V[(T, U)]

  def readTupleSizeSpecified[T, U](b: B, offset: O, tReader: (B, S) => V[T], uReader: (B, S) => V[U]): V[(T, U)]

  def writeTupleSizeSpecified[T, U](bb: B, v: V[(T, U)], tWrite: (B, V[T]) => S, uWrite: (B, V[U]) => S): S

  def writeTupleSizeSpecified[T, U](
      bb: B,
      offset: O,
      v: V[(T, U)],
      tWrite: (B, V[T]) => S,
      uWrite: (B, V[U]) => S
  ): S

  def sizeOfSeqSizeSpecified[T](v: V[Seq[T]], size: V[T] => S): S
  def readSeqSizeSpecified[T: ClassTag](b: B, reader: (B, S) => V[T]): V[Seq[T]]
  def readSeqSizeSpecified[T: ClassTag](b: B, offset: O, reader: (B, S) => V[T]): V[Seq[T]]

  def writeSeqSizeSpecified[T](b: B, seq: V[Seq[T]], writer: (B, V[T]) => S)(implicit ct: ClassTag[T]): S
  def writeSeqSizeSpecified[T](b: B, offset: O, seq: V[Seq[T]], writer: (B, V[T]) => S)(implicit ct: ClassTag[T]): S

  def sizeOfBlobSizeSpecified(v: V[Blob]): S

  def readBlobSizeSpecified(b: B, size: S): V[Blob]
  def readBlobSizeSpecified(b: B, offset: O, size: S): V[Blob]

  def writeBlobSizeSpecified(b: B, v: V[Blob]): S
  def writeBlobSizeSpecified(b: B, offset: O, v: V[Blob]): S

}
