package org.yupana.core.types

import org.yupana.readerwriter.{ MemoryBuffer, MemoryBufferEvalReaderWriter }

class MemoryBufferStorableTest extends StorableTestBase {

  val memoryBufferUtils: BufUtils[MemoryBuffer] = new BufUtils[MemoryBuffer] {
    override def createBuffer(size: Int): MemoryBuffer = MemoryBuffer.allocateHeap(size)
    override def position(buf: MemoryBuffer): Int = buf.position()
    override def rewind(bb: MemoryBuffer): Unit = bb.rewind()
  }

  "MemoryBuffer" should behave like storableTest(MemoryBufferEvalReaderWriter, memoryBufferUtils)

}
