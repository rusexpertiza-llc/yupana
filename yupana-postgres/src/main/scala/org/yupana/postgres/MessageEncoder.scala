package org.yupana.postgres

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToByteEncoder
import org.yupana.postgres.protocol.ServerMessage

class MessageEncoder extends MessageToByteEncoder[ServerMessage] {
  override def encode(ctx: ChannelHandlerContext, msg: ServerMessage, out: ByteBuf): Unit = {
    msg.write(out)
  }
}
