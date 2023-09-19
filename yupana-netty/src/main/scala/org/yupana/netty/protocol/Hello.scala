package org.yupana.netty.protocol

import io.netty.buffer.ByteBuf

trait Command

trait CommandHelper[C <: Command] {
  val tag: Byte
  def encode(c: C, out: ByteBuf): Unit
  def decode(b: ByteBuf): C
}

case class Hello(protocolVersion: Int, clientVersion: String) extends Command

object Hello extends CommandHelper[Hello] {
  override val tag: Byte = 1

  override def encode(c: Hello, out: ByteBuf): Unit = {
    out.writeByte(tag).writeInt(4).writeBytes("version".getBytes(Ch))
  }

  override def decode(b: ByteBuf): Hello = ???
}

object Command {}
