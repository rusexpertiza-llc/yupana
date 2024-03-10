package org.yupana.core.types

import org.scalacheck.Arbitrary
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.types.FixedStorable

class FixedStorableTest extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

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
