package org.yupana.api.utils

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GroupByIteratorTest extends AnyFlatSpec with Matchers {
  "GroupByIterator" should "group iterator values by key" in {
    val it = new GroupByIterator[String, Int](_.length, List("aaa", "bbb", "c", "dd", "ee", "fff").iterator)

    it.toList should contain theSameElementsInOrderAs List(
      3 -> Seq("aaa", "bbb"),
      1 -> Seq("c"),
      2 -> Seq("dd", "ee"),
      3 -> Seq("fff")
    )

  }

  it should "support empty input" in {
    new GroupByIterator[Int, Int](identity, Iterator.empty) shouldBe empty
  }
}
