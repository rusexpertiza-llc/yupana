package org.yupana.postgres

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ReplayingDecoder
import org.yupana.postgres.protocol.{ Message, SSLRequest }

import java.util

class MessageDecoder extends ReplayingDecoder[Message] {
  override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
    val len = in.readInt()
//    val vmaj = in.readShort()
//    val vmin = in.readShort()
//    println(s"GOT Hello $len from $vmaj:$vmin")
    val v = in.readInt()

    if (v == 80877103) {
      println(s"GOT SSL Request")
      out.add(SSLRequest)
    }
    in.skipBytes(len - 8)
  }
}
