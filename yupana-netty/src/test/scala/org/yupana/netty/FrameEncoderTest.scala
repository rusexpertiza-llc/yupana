package org.yupana.netty

import io.netty.buffer.Unpooled
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.netty.protocol.{ Hello, ReadWrite }

class FrameEncoderTest extends AnyFlatSpec with Matchers {

  "FrameEncoder" should "encode frame" in {
    val input = Unpooled.buffer()

    val cmd = Hello(1, "40.0.0", Map.empty)

    val rw = implicitly[ReadWrite[Hello]]
    rw.write(input, cmd)

  }
}
