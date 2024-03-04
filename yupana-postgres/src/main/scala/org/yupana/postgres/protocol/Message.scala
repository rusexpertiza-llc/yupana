package org.yupana.postgres.protocol

import io.netty.buffer.ByteBuf

trait Message

trait ClientMessage extends Message

case object SSLRequest extends ClientMessage

trait ServerMessage extends Message {
  def write(byteBuf: ByteBuf): Unit
}

case object No extends ServerMessage {
  override def write(byteBuf: ByteBuf): Unit = byteBuf.writeByte('N'.toByte)
}
