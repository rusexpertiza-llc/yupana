package org.yupana.netty

import io.netty.buffer.ByteBuf
import io.netty.channel.embedded.EmbeddedChannel
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.protocol.{ Frame, Hello, HelloResponse, ProtocolVersion, Tags }

class MessageHandlerTest extends AnyFlatSpec with Matchers {

  import NettyBuffer._

  "MessageHandler" should "process hello command" in {
    val ch = new EmbeddedChannel(new MessageHandler(1, 2, "1.2.3"))

    val cmd = new Hello(ProtocolVersion.value, "3.2.1", 1234567L, Map("batchSize" -> "1000"))
    val frame = cmd.toFrame[ByteBuf]

    ch.writeInbound(frame) // shouldBe true
    ch.finish() shouldBe true

    val respFrame = ch.readOutbound[Frame]()
    respFrame.frameType shouldEqual Tags.HELLO_RESPONSE
    val resp = HelloResponse.readFrame(respFrame)

    resp.protocolVersion shouldBe ProtocolVersion.value
    resp.reqTime shouldEqual 1234567L
  }
}
