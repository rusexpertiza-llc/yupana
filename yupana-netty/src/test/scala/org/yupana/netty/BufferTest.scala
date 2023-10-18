package org.yupana.netty

import io.netty.buffer.ByteBuf
import org.scalacheck.Arbitrary
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.yupana.protocol.Buffer

import java.nio.ByteBuffer

class BufferTest extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  import NettyBuffer._

  "Buffer" should "pass int from NIO to Netty" in writeOneReadAnother[Int, ByteBuffer, ByteBuf](_.writeInt, _.readInt)
  it should "pass int from Netty to NIO" in writeOneReadAnother[Int, ByteBuf, ByteBuffer](_.writeInt, _.readInt)

  it should "pass byte from NIO to Netty" in writeOneReadAnother[Byte, ByteBuffer, ByteBuf](_.writeByte, _.readByte)
  it should "pass byte from Netty to NIO" in writeOneReadAnother[Byte, ByteBuf, ByteBuffer](_.writeByte, _.readByte)

  it should "pass long from NIO to Netty" in writeOneReadAnother[Long, ByteBuffer, ByteBuf](_.writeLong, _.readLong)
  it should "pass long from Netty to NIO" in writeOneReadAnother[Long, ByteBuf, ByteBuffer](_.writeLong, _.readLong)

  it should "pass double from NIO to Netty" in writeOneReadAnother[Double, ByteBuffer, ByteBuf](
    _.writeDouble,
    _.readDouble
  )
  it should "pass double from Netty to NIO" in writeOneReadAnother[Double, ByteBuf, ByteBuffer](
    _.writeDouble,
    _.readDouble
  )

//  it should "pass string from NIO to Netty" in writeOneReadAnother[String, ByteBuffer, ByteBuf](
//    _.writeString,
//    _.readString
//  )
//  it should "pass string from Netty to NIO" in writeOneReadAnother[String, ByteBuf, ByteBuffer](
//    _.writeString,
//    _.readString
//  )

  private def writeOneReadAnother[T, B1, B2](
      w: Buffer[B1] => (B1, T) => Unit,
      r: Buffer[B2] => B2 => T
  )(implicit a: Arbitrary[T], B1: Buffer[B1], B2: Buffer[B2]): Assertion = {
    forAll { t: T =>
      val b1 = B1.alloc(1000)
      w(B1)(b1, t)
      val data = B1.getBytes(b1)

      val b2 = B2.wrap(data)
      r(B2)(b2) shouldEqual t
    }
  }

}
