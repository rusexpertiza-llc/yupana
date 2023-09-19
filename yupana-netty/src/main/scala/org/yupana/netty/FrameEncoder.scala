package org.yupana.netty

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToByteEncoder

class FrameEncoder extends MessageToByteEncoder[Frame] {
  override def encode(ctx: ChannelHandlerContext, frame: Frame, out: ByteBuf): Unit = {
    out
      .writeByte(frame.frameType)
      .writeInt(frame.payload.readableBytes())
      .writeBytes(frame.payload)
  }
}
