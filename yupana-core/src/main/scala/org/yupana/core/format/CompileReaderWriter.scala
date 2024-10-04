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

package org.yupana.core.format

import org.threeten.extra.PeriodDuration
import org.yupana.api.{ Blob, Currency, Time }
import org.yupana.api.types.InternalReaderWriter
import org.yupana.core.jit.codegen.CommonGen

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object CompileReaderWriter extends InternalReaderWriter[Tree, TypedTree, Tree, Tree] with Serializable {

  private val evalRW = q"_root_.org.yupana.serialization.MemoryBufferEvalReaderWriter"

  override def readBytes(b: Tree, d: TypedTree[Array[Byte]]): TypedTree[Unit] = {
    q"$evalRW.readBytes($b, $d)"
  }

  override def readBytes(b: Tree, offset: Tree, d: TypedTree[Array[Byte]]): TypedTree[Unit] = {
    q"$evalRW.readBytes($b, $offset, $d)"
  }

  override def writeBytes(b: Tree, v: TypedTree[Array[Byte]]): TypedTree[Int] = {
    q"$evalRW.writeBytes($b, $v)"
  }

  override def writeBytes(b: Tree, offset: Tree, v: TypedTree[Array[Byte]]): TypedTree[Int] = {
    q"$evalRW.writeBytes($b, $offset, $v)"
  }

  override def sizeOfInt: Tree = {
    q"$evalRW.sizeOfInt"
  }

  override def readInt(b: Tree): TypedTree[Int] = {
    q"$evalRW.readInt($b)"
  }

  override def readInt(b: Tree, offset: Tree): TypedTree[Int] = {
    q"$evalRW.readInt($b, $offset)"
  }

  override def writeInt(b: Tree, v: TypedTree[Int]): TypedTree[Int] = {
    q"$evalRW.writeInt($b, $v)"
  }

  override def writeInt(b: Tree, offset: Tree, v: TypedTree[Int]): TypedTree[Int] = {
    q"$evalRW.writeInt($b, $offset, $v)"
  }

  override def sizeOfLong: Tree = {
    q"$evalRW.sizeOfLong"
  }

  override def readLong(b: Tree): TypedTree[Long] = {
    q"$evalRW.readLong($b)"
  }

  override def readLong(b: Tree, offset: Tree): TypedTree[Long] = {
    q"$evalRW.readLong($b, $offset)"
  }

  override def writeLong(b: Tree, v: TypedTree[Long]): TypedTree[Long] = {
    q"$evalRW.writeLong($b, $v)"

  }

  override def writeLong(b: Tree, offset: Tree, v: TypedTree[Long]): TypedTree[Long] = {
    q"$evalRW.writeLong($b, $offset, $v)"

  }

  override def sizeOfDouble: Tree = {
    q"$evalRW.sizeOfDouble"
  }

  override def readDouble(b: Tree): TypedTree[Double] = {
    q"$evalRW.readDouble($b)"
  }

  override def readDouble(b: Tree, offset: Tree): TypedTree[Double] = {
    q"$evalRW.readDouble($b, $offset)"
  }

  override def writeDouble(b: Tree, v: TypedTree[Double]): TypedTree[Double] = {
    q"$evalRW.writeDouble($b, $v)"
  }

  override def writeDouble(b: Tree, offset: Tree, v: TypedTree[Double]): TypedTree[Double] = {
    q"$evalRW.writeDouble($b, $offset, $v)"
  }

  override def sizeOfShort: Tree = {
    q"$evalRW.sizeOfShort"
  }

  override def readShort(b: Tree): TypedTree[Short] = {
    q"$evalRW.readShort($b)"
  }

  override def readShort(b: Tree, offset: Tree): TypedTree[Short] = {
    q"$evalRW.readShort($b, $offset)"
  }

  override def writeShort(b: Tree, v: TypedTree[Short]): TypedTree[Short] = {
    q"$evalRW.writeShort($b, $v)"
  }

  override def writeShort(b: Tree, offset: Tree, v: TypedTree[Short]): TypedTree[Short] = {
    q"$evalRW.writeShort($b, $offset, $v)"
  }

  override def sizeOfByte: Tree = {
    q"$evalRW.sizeOfByte"
  }

  override def readByte(b: Tree): TypedTree[Byte] = {
    q"$evalRW.readByte($b)"
  }

  override def readByte(b: Tree, offset: Tree): TypedTree[Byte] = {
    q"$evalRW.readByte($b, $offset)"
  }

  override def writeByte(b: Tree, v: TypedTree[Byte]): TypedTree[Byte] = {
    q"$evalRW.writeByte($b, $v)"
  }

  override def writeByte(b: Tree, offset: Tree, v: TypedTree[Byte]): TypedTree[Byte] = {
    q"$evalRW.writeByte($b, $offset, $v)"
  }

  override def sizeOfTime: Tree = {
    q"$evalRW.sizeOfTime"
  }

  override def readTime(b: Tree): TypedTree[Time] = {
    q"$evalRW.readTime($b)"
  }

  override def readTime(b: Tree, offset: Tree): TypedTree[Time] = {
    q"$evalRW.readTime($b, $offset)"
  }

  override def writeTime(b: Tree, v: TypedTree[Time]): TypedTree[Time] = {
    q"$evalRW.writeTime($b, $v)"
  }
  override def writeTime(b: Tree, offset: Tree, v: TypedTree[Time]): TypedTree[Time] = {
    q"$evalRW.writeTime($b, $offset, $v)"
  }

  override def sizeOfTuple[T, U](
      v: TypedTree[(T, U)],
      tSize: TypedTree[T] => Tree,
      uSize: TypedTree[U] => Tree
  ): Tree = {
    val t = tSize(q"$v._1")
    val u = uSize(q"$v._3")
    q"$t + $u"
  }

  override def readTuple[T, U](
      b: Tree,
      tReader: Tree => TypedTree[T],
      uReader: Tree => TypedTree[U]
  ): TypedTree[(T, U)] = {
    val t = tReader(b)
    val u = uReader(b)
    q"($t, $u)"
  }

  override def readTuple[T, U](
      b: Tree,
      offset: Tree,
      tReader: Tree => TypedTree[T],
      uReader: Tree => TypedTree[U]
  ): TypedTree[(T, U)] = {
    q"""
       val bb = $b.slice($offset, $b.limit() - $offset)
       (${tReader(q"bb")}, ${uReader(q"bb")})
       """
  }

  override def writeTuple[T, U](
      b: Tree,
      v: TypedTree[(T, U)],
      tWrite: (Tree, TypedTree[T]) => TypedTree[T],
      uWrite: (Tree, TypedTree[U]) => TypedTree[U]
  ): TypedTree[(T, U)] = {
    val t = tWrite(b, q"$v._1")
    val u = uWrite(b, q"$v._2")
    q"""
       $t + $u
       """
  }

  override def writeTuple[T, U](
      b: Tree,
      offset: Tree,
      v: TypedTree[(T, U)],
      tWrite: (Tree, TypedTree[T]) => TypedTree[T],
      uWrite: (Tree, TypedTree[U]) => TypedTree[U]
  ): TypedTree[(T, U)] = {
    q"""
       val bb = $b.asSlice($offset, $b.size - $offset)
       ${tWrite(q"bb", q"$v._1")} + ${uWrite(q"bb", q"$v._2")}
       """
  }

  override def sizeOfTupleSizeSpecified[T, U](
      v: TypedTree[(T, U)],
      tSize: TypedTree[T] => Tree,
      uSize: TypedTree[U] => Tree
  ): Tree = {
    val t = tSize(q"$v._1")
    val u = uSize(q"$v._3")
    q"4 + $t + 4 + $u"
  }

  override def readTupleSizeSpecified[T, U](
      b: Tree,
      tReader: (Tree, Tree) => TypedTree[T],
      uReader: (Tree, Tree) => TypedTree[U]
  ): TypedTree[(T, U)] = {
    val ts = q"$b.getInt()"
    val t = tReader(b, ts)
    val us = q"$b.getInt()"
    val u = uReader(b, us)
    q"($t, $u)"
  }

  override def readTupleSizeSpecified[T, U](
      b: Tree,
      offset: Tree,
      tReader: (Tree, Tree) => TypedTree[T],
      uReader: (Tree, Tree) => TypedTree[U]
  ): TypedTree[(T, U)] = {
    q"""
        val bb = $b.asSlice(offset)
        val tSize = bb.getInt()
        val t = ${tReader(q"bb", q"tSize")}
        val uSize = bb.getInt()
        val u = ${uReader(q"bb", q"uSize")}
        (t, u)
       """
  }

  override def writeTupleSizeSpecified[T, U](
      b: Tree,
      v: TypedTree[(T, U)],
      tWrite: (Tree, TypedTree[T]) => Tree,
      uWrite: (Tree, TypedTree[U]) => Tree
  ): Tree = {
    q"""
        val pos = $b.position()
        $b.position(pos + 4)
        val tSize = ${tWrite(b, q"$v._1")}
        $b.putInt(pos, tSize)
        $b.position(pos + 4 + tSize + 4)
        val uSize = ${uWrite(b, q"$v._2")}
        $b.putInt(pos + 4 + uSize)
        4 + tSize + 4 + uSize
       """
  }

  override def writeTupleSizeSpecified[T, U](
      bb: Tree,
      offset: Tree,
      v: TypedTree[(T, U)],
      tWrite: (Tree, TypedTree[T]) => Tree,
      uWrite: (Tree, TypedTree[U]) => Tree
  ): Tree = {
    q"""
       val tb = $bb.asSlice($offset)
       ${writeTupleSizeSpecified(q"tb", v, tWrite, uWrite)}
       """
  }

  override def sizeOfBoolean: Tree = {
    q"$evalRW.sizeOfBoolean"
  }

  override def readBoolean(b: Tree): TypedTree[Boolean] = {
    q"$evalRW.readBoolean($b)"
  }

  override def readBoolean(b: Tree, offset: Tree): TypedTree[Boolean] = {
    q"$evalRW.readBoolean($b, $offset)"
  }

  override def writeBoolean(b: Tree, v: TypedTree[Boolean]): TypedTree[Boolean] = {
    q"$evalRW.writeBoolean($b, $v)"
  }

  override def writeBoolean(b: Tree, offset: Tree, v: TypedTree[Boolean]): TypedTree[Boolean] = {
    q"$evalRW.writeBoolean($b, $offset, $v)"
  }

  override def sizeOfString(v: TypedTree[String]): Tree = {
    q"$evalRW.sizeOfString($v)"
  }

  override def readString(b: Tree): TypedTree[String] = {
    q"$evalRW.readString($b)"
  }

  override def readString(b: Tree, offset: Tree): TypedTree[String] = {
    q"$evalRW.readString($b, $offset)"
  }

  override def writeString(b: Tree, v: TypedTree[String]): TypedTree[String] = {
    q"$evalRW.writeString($b, $v)"
  }

  override def writeString(b: Tree, offset: Tree, v: TypedTree[String]): TypedTree[String] = {
    q"$evalRW.writeString($b, $offset, $v)"
  }

  override def sizeOfStringSizeSpecified(v: TypedTree[String]): Tree = {
    q"$evalRW.sizeOfStringSizeSpecified($v)"
  }

  override def readStringSizeSpecified(b: Tree, size: Tree): TypedTree[String] = {
    q"$evalRW.readStringSizeSpecified($b, $size)"
  }

  override def readStringSizeSpecified(b: Tree, offset: Tree, size: Tree): TypedTree[String] = {
    q"$evalRW.readStringSizeSpecified($b, $offset, $size)"
  }

  override def writeStringSizeSpecified(b: Tree, v: TypedTree[String]): Tree = {
    q"$evalRW.writeStringSizeSpecified($b, $v)"
  }

  override def writeStringSizeSpecified(b: Tree, offset: Tree, v: TypedTree[String]): Tree = {
    q"$evalRW.writeStringSizeSpecified($b, $offset, $v)"
  }

  override def readVLong(b: Tree): TypedTree[Long] = {
    q"$evalRW.readVLong($b)"
  }

  override def readVLong(b: Tree, offset: Tree): TypedTree[Long] = {
    q"$evalRW.readVLong($b, $offset)"
  }

  override def writeVLong(b: Tree, v: TypedTree[Long]): TypedTree[Long] = {
    q"$evalRW.writeVLong($b, $v)"
  }

  override def writeVLong(b: Tree, offset: Tree, v: TypedTree[Long]): TypedTree[Long] = {
    q"$evalRW.writeVLong($b, $offset, $v)"
  }

  override def readVInt(b: Tree): TypedTree[Int] = {
    q"$evalRW.readVInt($b)"
  }

  override def readVInt(b: Tree, offset: Tree): TypedTree[Int] = {
    q"$evalRW.readVInt($b, $offset)"
  }

  override def writeVInt(b: Tree, v: TypedTree[Int]): TypedTree[Int] = {
    q"$evalRW.writeVInt($b, $v)"
  }

  override def writeVInt(b: Tree, offset: Tree, v: TypedTree[Int]): TypedTree[Int] = {
    q"$evalRW.writeVInt($b, $offset, $v)"
  }

  override def readVShort(b: Tree): TypedTree[Short] = {
    q"$evalRW.readVShort($b)"
  }

  override def readVShort(b: Tree, offset: Tree): TypedTree[Short] = {
    q"$evalRW.readVShort($b, $offset)"
  }

  override def writeVShort(b: Tree, v: TypedTree[Short]): TypedTree[Short] = {
    q"$evalRW.writeVShort($b, $v)"
  }

  override def writeVShort(b: Tree, offset: Tree, v: TypedTree[Short]): TypedTree[Short] = {
    q"$evalRW.writeVShort($b, $offset, $v)"
  }

  override def sizeOfBigDecimal(v: TypedTree[BigDecimal]): Tree = {
    q"$evalRW.sizeOfBigDecimal($v)"
  }

  override def readBigDecimal(b: Tree): TypedTree[BigDecimal] = {
    q"$evalRW.readBigDecimal($b)"
  }

  override def readBigDecimal(b: Tree, offset: Tree): TypedTree[BigDecimal] = {
    q"$evalRW.readBigDecimal($b, $offset)"
  }

  override def writeBigDecimal(b: Tree, v: TypedTree[BigDecimal]): TypedTree[BigDecimal] = {
    q"$evalRW.writeBigDecimal($b, $v)"
  }

  override def writeBigDecimal(b: Tree, offset: Tree, v: TypedTree[BigDecimal]): TypedTree[BigDecimal] = {
    q"$evalRW.writeBigDecimal($b, $offset, $v)"
  }

  override def sizeOfBigDecimalSizeSpecified(v: TypedTree[BigDecimal]): Tree = {
    q"$evalRW.sizeOfBigDecimalSizeSpecified($v)"
  }

  override def readBigDecimalSizeSpecified(b: Tree, size: Tree): TypedTree[BigDecimal] = {
    q"$evalRW.readBigDecimalSizeSpecified($b, $size)"
  }

  override def readBigDecimalSizeSpecified(b: Tree, offset: Tree, size: Tree): TypedTree[BigDecimal] = {
    q"$evalRW.readBigDecimalSizeSpecified($b, $offset, $size)"
  }

  override def writeBigDecimalSizeSpecified(b: Tree, v: TypedTree[BigDecimal]): TypedTree[BigDecimal] = {
    q"$evalRW.writeBigDecimalSizeSpecified($b, $v)"
  }

  override def writeBigDecimalSizeSpecified(b: Tree, offset: Tree, v: TypedTree[BigDecimal]): TypedTree[BigDecimal] = {
    q"$evalRW.writeBigDecimalSizeSpecified($b, $offset, $v)"
  }

  override def readVTime(b: Tree): TypedTree[Time] = {
    q"$evalRW.readVTime($b)"
  }

  override def readVTime(b: Tree, offset: Tree): TypedTree[Time] = {
    q"$evalRW.readVTime($b, $offset)"
  }

  override def writeVTime(b: Tree, v: TypedTree[Time]): TypedTree[Time] = {
    q"$evalRW.writeVTime($b, $v)"
  }

  override def writeVTime(b: Tree, offset: Tree, v: TypedTree[Time]): TypedTree[Time] = {
    q"$evalRW.writeVTime($b, $offset, $v)"
  }

  override def sizeOfSeq[T](v: TypedTree[Seq[T]], size: TypedTree[T] => Tree): Tree = {
    q"$evalRW.sizeOfSeq($v, (vv) => {${size(q"vv")}})"
  }

  override def readSeq[T: ClassTag](b: Tree, reader: Tree => TypedTree[T]): TypedTree[Seq[T]] = {
    q"$evalRW.readSeq($b, (rb) => {${reader(q"rb")}})"
  }

  override def readSeq[T: ClassTag](
      b: Tree,
      offset: Tree,
      reader: Tree => TypedTree[T]
  ): TypedTree[Seq[T]] = {
    q"""
       val r = $evalRW.readSeq($b, $offset, (rb) => {${reader(q"rb")}})
       """
  }

  override def writeSeq[T](
      b: Tree,
      seq: TypedTree[Seq[T]],
      writer: (Tree, TypedTree[T]) => TypedTree[T]
  )(implicit ct: ClassTag[T]): TypedTree[Seq[T]] = {
    val tpe = Ident(TypeName(CommonGen.className(ct)))
    q"$evalRW.writeSeq($b, $seq, (rb, v: _root_.org.yupana.api.types.ID[$tpe]) => {${writer(q"rb", q"v")}})"
  }

  override def writeSeq[T](
      b: Tree,
      offset: Tree,
      seq: TypedTree[Seq[T]],
      writer: (Tree, TypedTree[T]) => TypedTree[T]
  )(implicit ct: ClassTag[T]): TypedTree[Seq[T]] = {
    val tpe = Ident(TypeName(CommonGen.className(ct)))
    q"""
        $evalRW.writeSeq($b, $offset, $seq, (rb, v: _root_.org.yupana.api.types.ID[$tpe]) => {${writer(q"rb", q"v")}})
       """
  }

  override def sizeOfSeqSizeSpecified[T](v: TypedTree[Seq[T]], size: TypedTree[T] => Tree): Tree = {
    q"$evalRW.sizeOfSeqSizeSpecified($v, (vv) => {${size(q"vv")}})"

  }

  override def readSeqSizeSpecified[T: ClassTag](b: Tree, reader: (Tree, Tree) => TypedTree[T]): TypedTree[Seq[T]] = {
    q"$evalRW.readSeqSizeSpecified($b, (rb, s) => {${reader(q"rb", q"s")}})"
  }

  override def readSeqSizeSpecified[T: ClassTag](
      b: Tree,
      offset: Tree,
      reader: (Tree, Tree) => TypedTree[T]
  ): TypedTree[Seq[T]] = {
    q"""
       val r = $evalRW.readSeqSizeSpecified($b, $offset, (rb, s) => {${reader(q"rb", q"s")}})
     """
  }

  override def writeSeqSizeSpecified[T](b: Tree, seq: TypedTree[Seq[T]], writer: (Tree, TypedTree[T]) => Tree)(
      implicit ct: ClassTag[T]
  ): Tree = {
    val tpe = Ident(TypeName(CommonGen.className(ct)))
    q"$evalRW.writeSeqSizeSpecified($b, $seq, (rb, v: _root_.org.yupana.api.types.ID[$tpe]) => {${writer(q"rb", q"v")}})"
  }

  override def writeSeqSizeSpecified[T](
      b: Tree,
      offset: Tree,
      seq: TypedTree[Seq[T]],
      writer: (Tree, TypedTree[T]) => Tree
  )(
      implicit ct: ClassTag[T]
  ): Tree = {
    val tpe = Ident(TypeName(CommonGen.className(ct)))
    q"""
        $evalRW.writeSeqSizeSpecified($b, $offset, $seq, (rb, v: _root_.org.yupana.api.types.ID[$tpe]) => {${writer(
        q"rb",
        q"v"
      )}})
       """
  }

  override def sizeOfBlob(v: TypedTree[Blob]): Tree = {
    q"$evalRW.sizeOfBlob($v)"
  }

  override def readBlob(b: Tree): TypedTree[Blob] = {
    q"$evalRW.readBlob($b)"
  }

  override def readBlob(b: Tree, offset: Tree): TypedTree[Blob] = {
    q"$evalRW.readBlob($b, $offset)"
  }

  override def writeBlob(b: Tree, v: TypedTree[Blob]): TypedTree[Blob] = {
    q"$evalRW.writeBlob($b, $v)"
  }

  override def writeBlob(b: Tree, offset: Tree, v: TypedTree[Blob]): TypedTree[Blob] = {
    q"$evalRW.writeBlob($b, $offset, $v)"
  }

  override def sizeOfBlobSizeSpecified(v: TypedTree[Blob]): Tree = {
    q"$evalRW.sizeOfBlobSizeSpecified($v)"
  }

  override def readBlobSizeSpecified(b: Tree, size: Tree): TypedTree[Blob] = {
    q"$evalRW.readBlobSizeSpecified($b, $size)"
  }

  override def readBlobSizeSpecified(b: Tree, offset: Tree, size: Tree): TypedTree[Blob] = {
    q"$evalRW.readBlob($b, $offset, $size)"
  }

  override def writeBlobSizeSpecified(b: Tree, v: TypedTree[Blob]): Tree = {
    q"$evalRW.writeBlobSizeSpecified($b, $v)"
  }

  override def writeBlobSizeSpecified(b: Tree, offset: Tree, v: TypedTree[Blob]): Tree = {
    q"$evalRW.writeBlobSizeSpecified($b, $offset, $v)"
  }

  override def sizeOfPeriodDuration(v: TypedTree[PeriodDuration]): Tree = {
    q"$evalRW.sizeOfPeriodDuration($v)"
  }

  override def readPeriodDuration(b: Tree): TypedTree[PeriodDuration] = {
    q"$evalRW.readPeriodDuration($b)"
  }

  override def readPeriodDuration(b: Tree, offset: Tree): TypedTree[PeriodDuration] = {
    q"$evalRW.readPeriodDuration($b, $offset)"
  }

  override def writePeriodDuration(b: Tree, v: TypedTree[PeriodDuration]): TypedTree[PeriodDuration] = {
    q"$evalRW.writePeriodDuration($b, $v)"
  }
  override def writePeriodDuration(b: Tree, offset: Tree, v: TypedTree[PeriodDuration]): TypedTree[PeriodDuration] = {
    q"$evalRW.writePeriodDuration($b, $offset, $v)"
  }

  override def sizeOfCurrency: Tree = q"$evalRW.sizeOfCurrency"

  override def readCurrency(b: Tree): TypedTree[Currency] = q"$evalRW.readCurrency($b)"

  override def readCurrency(b: Tree, offset: Tree): TypedTree[Currency] = q"$evalRW.readCurrency($b, $offset)"

  override def writeCurrency(b: Tree, v: TypedTree[Currency]): Tree = q"$evalRW.writeCurrency($b, $v)"

  override def writeCurrency(b: Tree, offset: Tree, v: TypedTree[Currency]): Tree =
    q"$evalRW.writeCurrency($b, $offset, $v)"

  override def readVCurrency(b: Tree): TypedTree[Currency] = q"$evalRW.readVCurrency($b)"

  override def readVCurrency(b: Tree, offset: Tree): TypedTree[Currency] = q"$evalRW.readVCurrency($b, $offset)"

  override def writeVCurrency(b: Tree, v: TypedTree[Currency]): Tree = q"$evalRW.writeVCurrency(b, v)"

  override def writeVCurrency(b: Tree, offset: Tree, v: TypedTree[Currency]): Tree =
    q"$evalRW.writeVCurrency($b, $offset, $v)"
}
