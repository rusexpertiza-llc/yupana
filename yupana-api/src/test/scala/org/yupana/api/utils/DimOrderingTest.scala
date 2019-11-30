package org.yupana.api.utils

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class DimOrderingTest extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  val ord = implicitly[DimOrdering[Long]]

  "DimOrdering" should "support gt" in {
    forAll { (x: Long, y: Long) =>
      ord.gt(x, y) == (java.lang.Long.compareUnsigned(x, y) == 1)
    }
  }

  it should "support lt" in {
    forAll { (x: Long, y: Long) =>
      ord.lt(x, y) == (java.lang.Long.compareUnsigned(x, y) == -1)
    }
  }

  it should "support gte" in {
    forAll { (x: Long, y: Long) =>
      ord.gte(x, y) == (java.lang.Long.compareUnsigned(x, y) >= 0)
    }
  }

  it should "support lte" in {
    forAll { (x: Long, y: Long) =>
      ord.lte(x, y) == (java.lang.Long.compareUnsigned(x, y) <= 0)
    }
  }

  it should "support min" in {
    forAll { (x: Long, y: Long) =>
      val min = if (java.lang.Long.compareUnsigned(x, y) <= 0) x else y
      ord.min(x, y) == min
    }
  }

  it should "support max" in {
    forAll { (x: Long, y: Long) =>
      val max = if (java.lang.Long.compareUnsigned(x, y) >= 0) x else y
      ord.max(x, y) == max
    }
  }

}
