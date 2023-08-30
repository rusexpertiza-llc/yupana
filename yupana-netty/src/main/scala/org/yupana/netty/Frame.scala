package org.yupana.netty

case class Frame(frameType: Byte, length: Int, payload: Array[Byte])
