package org.yupana.netty

import io.netty.buffer.ByteBuf

case class Frame(frameType: Byte, length: Int, payload: ByteBuf)
