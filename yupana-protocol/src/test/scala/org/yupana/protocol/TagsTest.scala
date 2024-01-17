package org.yupana.protocol

import org.scalatest.Inspectors
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TagsTest extends AnyFlatSpec with Matchers with Inspectors {
  "Tags" should "be different" in {
    val tags = Tags.values.groupBy(_.value.toChar)

    forAll(tags) {
      case (_, ts) =>
        ts should have size 1
    }

  }
}
