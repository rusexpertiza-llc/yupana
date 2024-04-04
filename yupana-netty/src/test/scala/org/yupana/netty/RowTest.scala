package org.yupana.netty

import io.netty.buffer.Unpooled
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.types.{ DataType, ID }
import org.yupana.protocol.ResultRow
import org.yupana.serialization.ByteBufferEvalReaderWriter

import java.nio.ByteBuffer

class RowTest extends AnyFlatSpec with Matchers {

  "DataRow" should "serialize" in {
    val bb = Unpooled.buffer()
    val size = DataType[Int].storable.write(bb, 0, 367318: ID[Int])(ByteBufEvalReaderWriter)
    val a = Array.ofDim[Byte](size)
    bb.getBytes(0, a)
    val row = ResultRow(1, Seq(a))

    val bb2 = Unpooled.buffer()
    ResultRow.readWrite.write(bb2, row)(ByteBufEvalReaderWriter)

    val jbb = bb2.nioBuffer()

    val row2 = ResultRow.readWrite.read(jbb)(ByteBufferEvalReaderWriter)

    val x = DataType[Int].storable.read(ByteBuffer.wrap(row2.values.head))(ByteBufferEvalReaderWriter)

    println(s"x=$x")
  }

}
