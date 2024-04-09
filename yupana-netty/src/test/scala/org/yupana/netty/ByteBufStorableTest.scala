package org.yupana.netty

import io.netty.buffer.{ ByteBuf, Unpooled }
import org.yupana.core.types.StorableTestBase

class ByteBufStorableTest extends StorableTestBase {

  val byteBufUtils: BufUtils[ByteBuf] = new BufUtils[ByteBuf] {
    override def createBuffer(size: Int): ByteBuf = Unpooled.buffer(size)
    override def position(buf: ByteBuf): Int = buf.writerIndex()
    override def rewind(bb: ByteBuf): Unit = {} // bb.rewind()
  }

  "ByteBuf" should behave like storableTest(ByteBufEvalReaderWriter, byteBufUtils)

}
