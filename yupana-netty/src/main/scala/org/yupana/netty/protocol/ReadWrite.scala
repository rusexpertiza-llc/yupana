package org.yupana.netty.protocol

import io.netty.buffer.ByteBuf

import java.nio.charset.StandardCharsets

trait ReadWrite[T] {
  def read(buf: ByteBuf): T
  def write(buf: ByteBuf, t: T): Unit
}

object ReadWrite {
  implicit val rwInt: ReadWrite[Int] = new ReadWrite[Int] {
    override def read(buf: ByteBuf): Int = buf.readInt()
    override def write(buf: ByteBuf, t: Int): Unit = buf.writeInt(t)
  }

  implicit val rwString: ReadWrite[String] = new ReadWrite[String] {
    override def read(buf: ByteBuf): String = {
      val size = buf.readInt()
      buf.readCharSequence(size, StandardCharsets.UTF_8).toString
    }

    override def write(buf: ByteBuf, t: String): Unit = {
      val bytes = t.getBytes(StandardCharsets.UTF_8)
      buf.writeInt(bytes.length)
      buf.writeBytes(bytes)
    }
  }
}
