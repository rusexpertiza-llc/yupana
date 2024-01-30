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
import org.yupana.api.{ Blob, Time }
import org.yupana.api.types.ReaderWriter
import org.yupana.core.jit.codegen.CommonGen

import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.Tree
import scala.reflect.runtime.universe._

object CompileReaderWriter extends ReaderWriter[Tree, TypedTree, TypedTree] with Serializable {

  private val evalRW = q"_root_.org.yupana.readerwriter.MemoryBufferEvalReaderWriter"

  override def readBytes(b: Tree, d: TypedTree[Array[Byte]]): TypedTree[Unit] = {
    q"$evalRW.readBytes($b, $d)"
  }

  override def readBytes(b: Tree, offset: Int, d: TypedTree[Array[Byte]]): TypedTree[Unit] = {
    q"$evalRW.readBytes($b, $offset, $d)"
  }

  override def writeBytes(b: Tree, v: TypedTree[Array[Byte]]): TypedTree[Int] = {
    q"$evalRW.writeBytes($b, $v)"
  }

  override def writeBytes(b: Tree, offset: Int, v: TypedTree[Array[Byte]]): TypedTree[Int] = {
    q"$evalRW.writeBytes($b, $offset, $v)"
  }

  override def readInt(b: Tree): TypedTree[Int] = {
    q"$evalRW.readInt($b)"
  }

  override def readInt(b: Tree, offset: Int): TypedTree[Int] = {
    q"$evalRW.readInt($b, $offset)"
  }

  override def writeInt(b: Tree, v: TypedTree[Int]): TypedTree[Int] = {
    q"$evalRW.writeInt($b, $v)"
  }

  override def writeInt(b: Tree, offset: Int, v: TypedTree[Int]): TypedTree[Int] = {
    q"$evalRW.writeInt($b, $offset, $v)"
  }

  override def readLong(b: Tree): TypedTree[Long] = {
    q"$evalRW.readLong($b)"
  }

  override def readLong(b: Tree, offset: Int): TypedTree[Long] = {
    q"$evalRW.readLong($b, $offset)"
  }

  override def writeLong(b: Tree, v: TypedTree[Long]): TypedTree[Long] = {
    q"$evalRW.writeLong($b, $v)"

  }

  override def writeLong(b: Tree, offset: Int, v: TypedTree[Long]): TypedTree[Long] = {
    q"$evalRW.writeLong($b, $offset, $v)"

  }

  override def readDouble(b: Tree): TypedTree[Double] = {
    q"$evalRW.readDouble($b)"
  }

  override def readDouble(b: Tree, offset: Int): TypedTree[Double] = {
    q"$evalRW.readDouble($b, $offset)"
  }

  override def writeDouble(b: Tree, v: TypedTree[Double]): TypedTree[Double] = {
    q"$evalRW.writeDouble($b, $v)"
  }

  override def writeDouble(b: Tree, offset: Int, v: TypedTree[Double]): TypedTree[Double] = {
    q"$evalRW.writeDouble($b, $offset, $v)"
  }

  override def readShort(b: Tree): TypedTree[Short] = {
    q"$evalRW.readShort($b)"
  }

  override def readShort(b: Tree, offset: Int): TypedTree[Short] = {
    q"$evalRW.readShort($b, $offset)"
  }

  override def writeShort(b: Tree, v: TypedTree[Short]): TypedTree[Short] = {
    q"$evalRW.writeShort($b, $v)"
  }

  override def writeShort(b: Tree, offset: Int, v: TypedTree[Short]): TypedTree[Short] = {
    q"$evalRW.writeShort($b, $offset, $v)"
  }

  override def readByte(b: Tree): TypedTree[Byte] = {
    q"$evalRW.readByte($b)"
  }

  override def readByte(b: Tree, offset: Int): TypedTree[Byte] = {
    q"$evalRW.readByte($b, $offset)"
  }

  override def writeByte(b: Tree, v: TypedTree[Byte]): TypedTree[Byte] = {
    q"$evalRW.writeByte($b, $v)"
  }

  override def writeByte(b: Tree, offset: Int, v: TypedTree[Byte]): TypedTree[Byte] = {
    q"$evalRW.writeByte($b, $offset, $v)"
  }

  override def readTime(b: Tree): TypedTree[Time] = {
    q"$evalRW.readTime($b)"
  }

  override def readTime(b: Tree, offset: Int): TypedTree[Time] = {
    q"$evalRW.readTime($b, $offset)"
  }

  override def writeTime(b: Tree, v: TypedTree[Time]): TypedTree[Time] = {
    q"$evalRW.writeTime($b, $v)"
  }
  override def writeTime(b: Tree, offset: Int, v: TypedTree[Time]): TypedTree[Time] = {
    q"$evalRW.writeTime($b, $offset, $v)"
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
      offset: Int,
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
       $t
       $u
       """
  }

  override def writeTuple[T, U](
      b: Tree,
      offset: Int,
      v: TypedTree[(T, U)],
      tWrite: (Tree, TypedTree[T]) => TypedTree[T],
      uWrite: (Tree, TypedTree[U]) => TypedTree[U]
  ): TypedTree[(T, U)] = {
    q"""
       val bb = $b.slice($offset, $b.limit() - $offset)
       ${tWrite(q"bb", q"$v._1")}
       ${uWrite(q"bb", q"$v._2")}
       """
  }

  override def readBoolean(b: Tree): TypedTree[Boolean] = {
    q"$evalRW.readBoolean($b)"
  }
  override def readBoolean(b: Tree, offset: Int): TypedTree[Boolean] = {
    q"$evalRW.readBoolean($b, $offset)"
  }

  override def writeBoolean(b: Tree, v: TypedTree[Boolean]): TypedTree[Boolean] = {
    q"$evalRW.writeBoolean($b, $v)"
  }

  override def writeBoolean(b: Tree, offset: Int, v: TypedTree[Boolean]): TypedTree[Boolean] = {
    q"$evalRW.writeBoolean($b, $offset, $v)"
  }

  override def readString(b: Tree): TypedTree[String] = {
    q"$evalRW.readString($b)"
  }

  override def readString(b: Tree, offset: Int): TypedTree[String] = {
    q"$evalRW.readString($b, $offset)"
  }

  override def writeString(b: Tree, v: TypedTree[String]): TypedTree[String] = {
    q"$evalRW.writeString($b, $v)"
  }

  override def writeString(b: Tree, offset: Int, v: TypedTree[String]): TypedTree[String] = {
    q"$evalRW.writeString($b, $offset, $v)"
  }

  override def readVLong(b: Tree): TypedTree[Long] = {
    q"$evalRW.readVLong($b)"
  }

  override def readVLong(b: Tree, offset: Int): TypedTree[Long] = {
    q"$evalRW.readVLong($b, $offset)"
  }

  override def writeVLong(b: Tree, v: TypedTree[Long]): TypedTree[Long] = {
    q"$evalRW.writeVLong($b, $v)"
  }

  override def writeVLong(b: Tree, offset: Int, v: TypedTree[Long]): TypedTree[Long] = {
    q"$evalRW.writeVLong($b, $offset, $v)"
  }

  override def readVInt(b: Tree): TypedTree[Int] = {
    q"$evalRW.readVInt($b)"
  }

  override def readVInt(b: Tree, offset: Int): TypedTree[Int] = {
    q"$evalRW.readVInt($b, $offset)"
  }

  override def writeVInt(b: Tree, v: TypedTree[Int]): TypedTree[Int] = {
    q"$evalRW.writeVInt($b, $v)"
  }

  override def writeVInt(b: Tree, offset: Int, v: TypedTree[Int]): TypedTree[Int] = {
    q"$evalRW.writeVInt($b, $offset, $v)"
  }

  override def readVShort(b: Tree): TypedTree[Short] = {
    q"$evalRW.readVShort($b)"
  }

  override def readVShort(b: Tree, offset: Int): TypedTree[Short] = {
    q"$evalRW.readVShort($b, $offset)"
  }

  override def writeVShort(b: Tree, v: TypedTree[Short]): TypedTree[Short] = {
    q"$evalRW.writeVShort($b, $v)"
  }

  override def writeVShort(b: Tree, offset: Int, v: TypedTree[Short]): TypedTree[Short] = {
    q"$evalRW.writeVShort($b, $offset, $v)"
  }

  override def readBigDecimal(b: Tree): TypedTree[BigDecimal] = {
    q"$evalRW.readBigDecimal($b)"
  }

  override def readBigDecimal(b: Tree, offset: Int): TypedTree[BigDecimal] = {
    q"$evalRW.readBigDecimal($b, $offset)"
  }

  override def writeBigDecimal(b: Tree, v: TypedTree[BigDecimal]): TypedTree[BigDecimal] = {
    q"$evalRW.writeBigDecimal($b, $v)"
  }

  override def writeBigDecimal(b: Tree, offset: Int, v: TypedTree[BigDecimal]): TypedTree[BigDecimal] = {
    q"$evalRW.writeBigDecimal($b, $offset, $v)"
  }

  override def readVTime(b: Tree): TypedTree[Time] = {
    q"$evalRW.readVTime($b)"
  }

  override def readVTime(b: Tree, offset: Int): TypedTree[Time] = {
    q"$evalRW.readVTime($b, $offset)"
  }

  override def writeVTime(b: Tree, v: TypedTree[Time]): TypedTree[Time] = {
    q"$evalRW.writeVTime($b, $v)"
  }

  override def writeVTime(b: Tree, offset: Int, v: TypedTree[Time]): TypedTree[Time] = {
    q"$evalRW.writeVTime($b, $offset, $v)"
  }

  override def readSeq[T: ClassTag](b: Tree, reader: Tree => TypedTree[T]): TypedTree[Seq[T]] = {
    q"$evalRW.readSeq($b, (rb) => {${reader(q"rb")}})"
  }

  override def readSeq[T: ClassTag](
      b: universe.Tree,
      offset: Int,
      reader: universe.Tree => TypedTree[T]
  ): TypedTree[Seq[T]] = {
    q"""
       val p = $b.position()
       $b.position($offset)
       val r = $evalRW.readSeq($b, (rb) => {${reader(q"rb")}})
       $b.position(p)
       r
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
      offset: Int,
      seq: TypedTree[Seq[T]],
      writer: (Tree, TypedTree[T]) => TypedTree[T]
  )(implicit ct: ClassTag[T]): TypedTree[Seq[T]] = {
    val tpe = CommonGen.className(ct)
    q"""
       val p = $b.position()
        $b.position($offset)
        val s = $evalRW.writeSeq($b, $seq, (rb, v: _root_.org.yupana.api.types.ID[$tpe]) => {${writer(q"rb", q"v")}})
        $b.position(p)
        s
       """
  }

  override def readBlob(b: Tree): TypedTree[Blob] = {
    q"$evalRW.readBlob($b)"
  }

  override def readBlob(b: Tree, offset: Int): TypedTree[Blob] = {
    q"$evalRW.readBlob($b, $offset)"
  }

  override def writeBlob(b: Tree, v: TypedTree[Blob]): TypedTree[Blob] = {
    q"$evalRW.writeBlob($b, $v)"
  }

  override def writeBlob(b: Tree, offset: Int, v: TypedTree[Blob]): TypedTree[Blob] = {
    q"$evalRW.writeBlob($b, $offset, $v)"
  }

  override def readPeriodDuration(b: Tree): TypedTree[PeriodDuration] = {
    q"$evalRW.readPeriodDuration($b)"
  }

  override def readPeriodDuration(b: Tree, offset: Int): TypedTree[PeriodDuration] = {
    q"$evalRW.readPeriodDuration($b, $offset)"
  }

  override def writePeriodDuration(b: Tree, v: TypedTree[PeriodDuration]): TypedTree[PeriodDuration] = {
    q"$evalRW.writePeriodDuration($b, $v)"
  }
  override def writePeriodDuration(b: Tree, offset: Int, v: TypedTree[PeriodDuration]): TypedTree[PeriodDuration] = {
    q"$evalRW.writePeriodDuration($b, $offset, $v)"
  }

}
