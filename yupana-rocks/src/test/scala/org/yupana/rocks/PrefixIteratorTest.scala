package org.yupana.rocks

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.schema.RawDimension
import org.yupana.api.utils.SortedSetIterator
import org.yupana.core.TestSchema

class PrefixIteratorTest extends AnyFlatSpec with Matchers {

  "A PrefixIterator" should "iterate on one entry set" in {
    val dimensionIterators = Seq(RawDimension[Long]("baseTime") -> SortedSetIterator(Seq(1L).iterator).prefetch(100))
    val it = new PrefixIterator(TestSchema.testTable, dimensionIterators)
    it.size shouldBe 1
  }

  "A PrefixIterator" should "iterate on Empty set" in {
    val dimensionIterators =
      Seq(RawDimension[Long]("baseTime") -> SortedSetIterator(Seq.empty[Long].iterator).prefetch(100))
    val it = new PrefixIterator(TestSchema.testTable, dimensionIterators)
    it.size shouldBe 0
  }

  "A PrefixIterator" should "iterate on more than one entry set" in {
    val dimensionIterators =
      Seq(RawDimension[Long]("baseTime") -> SortedSetIterator(Seq(1L, 2L).iterator).prefetch(100))
    val it = new PrefixIterator(TestSchema.testTable, dimensionIterators)
    it.size shouldBe 2
  }

}
