package org.yupana.core.types

import org.scalacheck.Arbitrary
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.yupana.api.types.{ FixedStorable, ID, ReaderWriter }
import org.yupana.readerwriter.ByteBufferEvalReaderWriter

import java.nio.ByteBuffer

class FixedStorableTest extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  implicit val rw: ReaderWriter[ByteBuffer, ID, Int, Int] = ByteBufferEvalReaderWriter

  "FixedStorable" should "handle Long values" in readWriteTest[Long]

  it should "handle Int values" in readWriteTest[Int]

  it should "handle Double values" in readWriteTest[Double]

  private def readWriteTest[T: FixedStorable: Arbitrary] = {
    val fs = implicitly[FixedStorable[T]]
    forAll { v: T =>
      val bb = ByteBuffer.allocate(1000)
      val posBeforeWrite = bb.position()
      val actualSize = fs.write(bb, v: ID[T])
      val posAfterWrite = bb.position()
      val expectedSize = posAfterWrite - posBeforeWrite
      expectedSize shouldEqual actualSize
      bb.rewind()
      fs.read(bb) shouldEqual v
    }
  }
}
