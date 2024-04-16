package org.yupana.core.types

import org.scalacheck.Arbitrary
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.yupana.api.types.{ ID, InternalReaderWriter, InternalStorable }
import org.yupana.api.{ Blob, Time }
import org.yupana.readerwriter.{ MemoryBuffer, MemoryBufferEvalReaderWriter }

class InternalStorableTest
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with TableDrivenPropertyChecks {

  implicit val rw: InternalReaderWriter[MemoryBuffer, ID, Int, Int] = MemoryBufferEvalReaderWriter

  implicit private val genTime: Arbitrary[Time] = Arbitrary(Arbitrary.arbitrary[Long].map(Time.apply))

  implicit private val genBlob: Arbitrary[Blob] = Arbitrary(Arbitrary.arbitrary[Array[Byte]].map(Blob.apply))

  "Serialization" should "preserve doubles on write read cycle" in readWriteTest[Double]

  it should "preserve Ints on write read cycle" in readWriteTest[Int]

  it should "preserve Longs on write read cycle" in readWriteTest[Long]

  it should "preserve BigDecimals on read write cycle" in readWriteTest[BigDecimal]

  it should "preserve Strings on read write cycle" in readWriteTest[String]

  it should "preserve Time on read write cycle" in readWriteTest[Time]

  it should "preserve Booleans on readwrite cycle" in readWriteTest[Boolean]

  it should "preserve sequences of Int on read write cycle" in readWriteTest[Seq[Int]]

  it should "preserve sequences of String on read write cycle" in readWriteTest[Seq[String]]

  it should "preserve BLOBs on read write cycle" in readWriteTest[Blob]

  private def readWriteTest[T: InternalStorable: Arbitrary] = {
    val storable = implicitly[InternalStorable[T]]

    forAll { t: T =>
      val bb = MemoryBuffer.allocateHeap(65535)
      val posBeforeWrite = bb.position()
      val actualSize = storable.write(bb, t: ID[T])
      val posAfterWrite = bb.position()
      val expectedSize = posAfterWrite - posBeforeWrite
      expectedSize shouldEqual actualSize
      bb.rewind()
      storable.read(bb, expectedSize) shouldEqual t

      storable.write(bb, 1000, t: ID[T])
      storable.read(bb, 1000, expectedSize) shouldEqual t

      storable.size(t: ID[T]) shouldEqual expectedSize
    }
  }
}
