package org.yupana.netty

import io.netty.buffer.{ ByteBufUtil, Unpooled }
import io.netty.channel.embedded.EmbeddedChannel
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FrameCodecTest extends AnyFlatSpec with Matchers {

  "FrameCodec" should "encode/decode frame frame" in {
    val input = Unpooled.buffer()

    input.writeBytes("Hello frame".getBytes())
    val frame = Frame(10.toByte, input)

    val ch = new EmbeddedChannel(new FrameCodec())
    ch.writeInbound(frame) shouldBe true
    ch.finish() shouldBe true

    val result = ch.readInbound[Frame]()
    result.frameType shouldBe 10.toByte
    new String(ByteBufUtil.getBytes(result.payload)) shouldEqual "Hello frame"
  }
}
