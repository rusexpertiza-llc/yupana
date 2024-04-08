package org.yupana.core.types

import org.yupana.readerwriter.ByteBufferEvalReaderWriter

import java.nio.ByteBuffer

class ByteBufferStorableTest extends StorableTestBase {

  val byteBufferUtils: BufUtils[ByteBuffer] = new BufUtils[ByteBuffer] {
    override def createBuffer(size: Int): ByteBuffer = ByteBuffer.allocate(size)
    override def position(buf: ByteBuffer): Int = buf.position()
    override def rewind(bb: ByteBuffer): Unit = bb.rewind()
  }

  "ByteBuffer" should behave like storableTest(ByteBufferEvalReaderWriter, byteBufferUtils)

}
