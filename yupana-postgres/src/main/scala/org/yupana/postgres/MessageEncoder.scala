package org.yupana.postgres

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToByteEncoder
import org.yupana.postgres.protocol.Message

class MessageEncoder extends MessageToByteEncoder[Message] {
  override def encode(ctx: ChannelHandlerContext, msg: Message, out: ByteBuf): Unit = ???
}
