package org.yupana.netty

import io.netty.buffer.{ ByteBuf, ByteBufUtil, Unpooled }
import io.netty.channel.embedded.EmbeddedChannel
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.protocol.Frame

class FrameCodecTest extends AnyFlatSpec with Matchers {
  import NettyBuffer._

  "FrameCodec" should "encode/decode frame frame" in {
    val input = Unpooled.buffer()

    input.writeBytes("Hello frame".getBytes())
    val frame = Frame(10.toByte, input)

    val ch = new EmbeddedChannel(new FrameCodec())
    ch.writeInbound(frame) shouldBe true
    ch.finish() shouldBe true

    val result = ch.readInbound[Frame[ByteBuf]]()
    result.frameType shouldBe 10.toByte
    new String(ByteBufUtil.getBytes(result.payload)) shouldEqual "Hello frame"
  }
}
