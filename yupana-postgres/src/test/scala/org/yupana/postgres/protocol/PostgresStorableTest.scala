package org.yupana.postgres.protocol

import io.netty.buffer.{ ByteBuf, Unpooled }
import org.yupana.core.types.StorableTestBase

import java.nio.charset.StandardCharsets

class PostgresStorableTest extends StorableTestBase {

  val byteBufUtils: BufUtils[ByteBuf] = new BufUtils[ByteBuf] {
    override def createBuffer(size: Int): ByteBuf = Unpooled.buffer(size)
    override def position(buf: ByteBuf): Int = buf.writerIndex()
    override def rewind(bb: ByteBuf): Unit = {} // bb.rewind()
  }

  "Postgres" should behave like storableTest(new PostgresBinaryReaderWriter(StandardCharsets.UTF_8), byteBufUtils)

  it should behave like stringStorageTest(PostgresStringReaderWriter)
}
