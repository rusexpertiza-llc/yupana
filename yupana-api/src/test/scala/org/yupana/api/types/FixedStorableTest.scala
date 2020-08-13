package org.yupana.api.types

import org.scalacheck.Arbitrary
import org.scalatest.{ FlatSpec, Matchers }
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class FixedStorableTest extends FlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  "FixedStorable" should "handle Long values" in readWriteTest[Long]

  it should "handle Int values" in readWriteTest[Int]

  it should "handle Double values" in readWriteTest[Double]

  private def readWriteTest[T: FixedStorable: Arbitrary] = {
    val fs = implicitly[FixedStorable[T]]
    forAll { v: T =>
      fs.read(fs.write(v)) shouldEqual v
    }
  }
}
