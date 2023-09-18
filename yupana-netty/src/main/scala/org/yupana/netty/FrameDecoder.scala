package org.yupana.netty

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ReplayingDecoder

import java.util

class FrameDecoder extends ReplayingDecoder[Frame] {
  override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
    val msgType = in.readByte()
    val len = in.readInt()
    val payload = in.readRetainedSlice(len)
    out.add(Frame(msgType, len, payload))
  }
}
