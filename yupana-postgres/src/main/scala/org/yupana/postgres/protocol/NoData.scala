package org.yupana.postgres.protocol
import io.netty.buffer.ByteBuf

import java.nio.charset.Charset

case object NoData extends TaggedServerMessage {

  override val tag: Byte = 'n'
  override def writePayload(buf: ByteBuf, charset: Charset): Unit = {}
}
