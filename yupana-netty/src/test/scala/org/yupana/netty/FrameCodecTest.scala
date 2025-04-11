package org.yupana.netty

import io.netty.buffer.{ ByteBuf, Unpooled }
import io.netty.channel.embedded.EmbeddedChannel
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.protocol.Frame

import java.nio.charset.StandardCharsets

class FrameCodecTest extends AnyFlatSpec with Matchers {

  "FrameDecoder" should "decode frame" in {
    val ch = new EmbeddedChannel(new FrameDecoder())
    ch.writeInbound(
      Unpooled.wrappedBuffer(Array[Byte](10, 0, 0, 0, 11) ++ "Hello frame".getBytes()).retain()
    ) shouldBe true
    ch.finish() shouldBe true

    val result = ch.readInbound[Frame[ByteBuf]]()
    result.frameType shouldBe 10.toByte
    result.payload.readCharSequence(result.payload.readableBytes(), StandardCharsets.UTF_8) shouldEqual "Hello frame"
  }

  "FrameEncoder" should "encode frame" in {
    val ch = new EmbeddedChannel(new FrameEncoder())
    ch.writeOutbound(Frame(42.toByte, Unpooled.wrappedBuffer(Array[Byte](1, 2, 3, 4))))

    val result = ch.readOutbound[ByteBuf]()
    result.readByte() shouldEqual 42
    val size = result.readInt()
    size shouldEqual 4
    val payload = new Array[Byte](size)
    result.readBytes(payload)
    payload should contain theSameElementsInOrderAs Array[Byte](1, 2, 3, 4)
  }
}
