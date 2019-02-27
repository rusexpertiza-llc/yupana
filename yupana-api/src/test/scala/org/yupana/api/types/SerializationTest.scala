package org.yupana.api.types

import org.scalacheck.Arbitrary
import org.scalatest.prop.{GeneratorDrivenPropertyChecks, TableDrivenPropertyChecks}
import org.scalatest.{FlatSpec, Matchers}
import org.yupana.api.Time

class SerializationTest extends FlatSpec
  with Matchers
  with GeneratorDrivenPropertyChecks
  with TableDrivenPropertyChecks {

  private val genTime = Arbitrary.arbitrary[Long].suchThat(_ >= 0).map(Time(_))

  "Serialization" should "preserve doubles on write read cycle" in {
    val readable = implicitly[Readable[Double]]
    val writable = implicitly[Writable[Double]]
    forAll { value: Double =>
      readable.read(writable.write(value)) shouldEqual value
    }
  }

  it should "preserve ints on write read cycle" in {
    val readable = implicitly[Readable[Int]]
    val writable = implicitly[Writable[Int]]
    forAll { value: Int =>
      readable.read(writable.write(value)) shouldEqual value
    }
  }

  it should "preserve longs on write read cycle" in {
    val readable = implicitly[Readable[Long]]
    val writable = implicitly[Writable[Long]]

    forAll { value: Long =>
      readable.read(writable.write(value)) shouldEqual value
    }
  }

  it should "preserve BigDecimals on read write cycle" in {
    val readable = implicitly[Readable[BigDecimal]]
    val writable = implicitly[Writable[BigDecimal]]
    forAll { value: BigDecimal =>
      readable.read(writable.write(value)) shouldEqual value
    }
  }

  it should "preserve Strings on read write cycle" in {
    val readable = implicitly[Readable[String]]
    val writable = implicitly[Writable[String]]
    forAll { value: String =>
      readable.read(writable.write(value)) shouldEqual value
    }
  }

  it should "preserve Time on read write cycle" in {
    val readable = implicitly[Readable[Time]]
    val writable = implicitly[Writable[Time]]
    forAll(genTime) { value: Time =>
      readable.read(writable.write(value)) shouldEqual value
    }
  }

  it should "compact numbers" in {
    val writable = implicitly[Writable[Long]]

    val table = Table(
      ("Value",     "Bytes count"),
      ( 0l,                   1),
      ( 100l,                 1),
      (-105l,                 1),
      ( 181l,                 2),
      (-222l,                 2),
      ( 1000l,                3),
      (-1000l,                3),
      ( 70000l,               4),
      (-70000l,               4),
      ( 3000000000l,          5),
      (-1099511627776L,       6),
      ( 1099511627776L,       7),
      ( 290000000000000L,     8),
      (-5000000000000000000L, 9),
      ( 1000000000000000000L, 9)
    )

    forAll(table) { (x, len) =>
      writable.write(x).length shouldEqual len
    }
  }

  it should "not read Long as Int if it overflows" in {
    val writable = implicitly[Writable[Long]]
    val readable = implicitly[Readable[Int]]

    an[IllegalArgumentException] should be thrownBy readable.read(writable.write(3000000000l))
  }
}
