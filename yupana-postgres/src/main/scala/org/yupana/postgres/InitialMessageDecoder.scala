package org.yupana.postgres

import com.typesafe.scalalogging.StrictLogging
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ReplayingDecoder
import org.yupana.postgres.protocol.{ Message, SSLRequest }

import java.nio.charset.{ Charset, StandardCharsets }
import java.util

class InitialMessageDecoder extends ReplayingDecoder[Message] with StrictLogging {

  def readNullTerminatedString(in: ByteBuf, charset: Charset): String = {
    val toEnd = in.bytesBefore(0.toByte)
    in.readCharSequence(toEnd, charset).toString
  }

  override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
    val len = in.readInt()
    println(s"GOT MESSAGE $len")
    val v = in.readInt()

    if (v == 80877103) {
      logger.info(s"Got SSL Request from ${ctx.channel().remoteAddress()}")
      println(s"GOT SSL Request")
      out.add(SSLRequest)
    } else {
      val vMaj = v >> 16
      val vMin = v & 0xFF
      println(s"StartupMessage, $vMaj.$vMin")

      val s = readNullTerminatedString(in, StandardCharsets.US_ASCII)
      println(s"GOT ${s}")

    }
    in.skipBytes(len - 8)
  }
}
