package org.yupana.api.types

import org.scalacheck.Arbitrary
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.yupana.api.{ Blob, Time }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StorableTest
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with TableDrivenPropertyChecks {

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

  it should "compact numbers" in {
    val storable = implicitly[Storable[Long]]

    val table = Table(
      ("Value", "Bytes count"),
      (0L, 1),
      (100L, 1),
      (-105L, 1),
      (181L, 2),
      (-222L, 2),
      (1000L, 3),
      (-1000L, 3),
      (70000L, 4),
      (-70000L, 4),
      (3000000000L, 5),
      (-1099511627776L, 6),
      (1099511627776L, 7),
      (290000000000000L, 8),
      (-5000000000000000000L, 9),
      (1000000000000000000L, 9)
    )

    forAll(table) { (x, len) =>
      storable.write(x).length shouldEqual len
    }
  }

  it should "not read Long as Int if it overflows" in {
    val longStorable = implicitly[Storable[Long]]
    val intStorable = implicitly[Storable[Int]]

    an[IllegalArgumentException] should be thrownBy intStorable.read(longStorable.write(3000000000L))
  }

  private def readWriteTest[T: Storable: Arbitrary] = {
    val storable = implicitly[Storable[T]]

    forAll { (t: T) =>
      storable.read(storable.write(t)) shouldEqual t
    }
  }
}
