package org.yupana.netty

import io.netty.channel.embedded.EmbeddedChannel
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.netty.protocol.{ Hello, HelloResponse, ProtocolVersion }

class MessageHandlerTest extends AnyFlatSpec with Matchers {

  "MessageHandler" should "process hello command" in {
    val ch = new EmbeddedChannel(new MessageHandler(1, 2, "1.2.3"))

    ch.writeInbound(new Hello(ProtocolVersion.value, "3.2.1", Map("batchSize" -> "1000"))) shouldBe true
    ch.finish() shouldBe true

    val resp = ch.readOutbound[HelloResponse]()

    resp.version shouldBe "1.2.3"
  }
}
