package org.yupana.postgres.protocol

import io.netty.buffer.{ ByteBuf, Unpooled }
import org.yupana.api.types.{ ByteReaderWriter, ID, Storable }
import org.yupana.core.types.StorableTestBase

import java.nio.charset.StandardCharsets

class PostgresStorableTest extends StorableTestBase {

  val byteBufUtils: BufUtils[ByteBuf] = new BufUtils[ByteBuf] {
    override def createBuffer(size: Int): ByteBuf = Unpooled.buffer(size)
    override def position(buf: ByteBuf): Int = buf.writerIndex()
    override def rewind(bb: ByteBuf): Unit = {} // bb.rewind()
  }

//  "Postgres" should behave like storableTest(new PostgresBinaryReaderWriter(StandardCharsets.UTF_8), byteBufUtils)
//
//  it should behave like stringStorageTest(PostgresStringReaderWriter)

  it should "properly BigDecimal(0+202)" in {
    val storable = implicitly[Storable[BigDecimal]]
    implicit val rw: ByteReaderWriter[ByteBuf] = new PostgresBinaryReaderWriter(StandardCharsets.UTF_8)

//    val t = BigDecimal(0L, 202)
    val t = BigDecimal(11.079634230747)

    println(s"T=$t")

    val bb = byteBufUtils.createBuffer(65535)
    val posBeforeWrite = byteBufUtils.position(bb)
    val actualSize = storable.write(bb, t: ID[BigDecimal])
    val posAfterWrite = byteBufUtils.position(bb)
    val expectedSize = posAfterWrite - posBeforeWrite
    expectedSize shouldEqual actualSize
    byteBufUtils.rewind(bb)
    storable.read(bb) shouldEqual t

    storable.write(bb, 1000, t: ID[BigDecimal]) shouldEqual expectedSize
    storable.read(bb, 1000) shouldEqual t
  }
}
