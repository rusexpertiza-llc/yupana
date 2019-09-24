package org.yupana.api.utils

import org.scalatest.{ FlatSpec, Matchers }
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class SortedIteratorTest extends FlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  "SortedSetIterator" should "throw IllegalStateException when items not sorted" in {

    val it = SortedSetIterator(1, 2, 2, 3, 4, 5)

    it.toList shouldBe List(1, 2, 3, 4, 5)

  }

  it should "throws IllegalSTateException when items not sorted" in {
    val it = SortedSetIterator(1, 2, 4, 3, 5)

    an[IllegalStateException] should be thrownBy it.sum

  }

  it should "remove duplicates" in {
    val it = SortedSetIterator(1, 1, 1, 2, 2, 2, 3, 4, 5, 6, 6, 6, 6)

    it.toList shouldBe List(1, 2, 3, 4, 5, 6)
  }

  it should "union iterators" in {
    SortedSetIterator[Int]().union(SortedSetIterator(1)).toList shouldBe List(1)

    SortedSetIterator(1).union(SortedSetIterator(1)).toList shouldBe List(1)
    SortedSetIterator(1, 2, 3).union(SortedSetIterator(1)).toList shouldBe List(1, 2, 3)
    SortedSetIterator(1).union(SortedSetIterator(1, 2, 3, 4)).toList shouldBe List(1, 2, 3, 4)
    SortedSetIterator(1, 1, 1, 5, 5, 5).union(SortedSetIterator(1, 2, 4)).toList shouldBe List(1, 2, 4, 5)
    SortedSetIterator(1, 1, 1, 5, 5, 5).union(SortedSetIterator(1, 2, 4, 5, 5, 5)).toList shouldBe List(1, 2, 4, 5)
  }

  it should "union any sorted iterators" in {
    forAll { (xs: List[Long], ys: List[Long]) =>
      val xit = xs.sorted.toIterator
      val yit = ys.sorted.toIterator

      val expected = xs.union(ys).distinct.sorted
      SortedSetIterator(xit).union(SortedSetIterator(yit)).toList shouldBe expected
    }
  }

  it should "intersect iterators" in {
    SortedSetIterator(0).intersect(SortedSetIterator(-1)).toList shouldBe List()
    SortedSetIterator[Int]().intersect(SortedSetIterator(1)).toList shouldBe List()
    SortedSetIterator(1).intersect(SortedSetIterator(1)).toList shouldBe List(1)
    SortedSetIterator(1, 2, 3).intersect(SortedSetIterator(1)).toList shouldBe List(1)
    SortedSetIterator(1).intersect(SortedSetIterator(1, 2, 3, 4)).toList shouldBe List(1)
    SortedSetIterator(1, 1, 1, 4, 5, 5, 5).intersect(SortedSetIterator(1, 2, 4)).toList shouldBe List(1, 4)
    SortedSetIterator(1, 1, 1, 5, 5, 5).intersect(SortedSetIterator(1, 2, 4, 5, 5, 5)).toList shouldBe List(1, 5)
  }

  it should "intersect any sorted iterators" in {
    forAll { (xs: List[Long], ys: List[Long]) =>
      val xit = xs.sorted.toIterator
      val yit = ys.sorted.toIterator

      val expected = xs.toSet.intersect(ys.toSet).toList.sorted
      SortedSetIterator(xit).intersect(SortedSetIterator(yit)).toList shouldBe expected
    }
  }

  it should "exclude sorted iterator" in {
    SortedSetIterator[Int]().exclude(SortedSetIterator[Int]()).toList shouldBe List()
    SortedSetIterator[Int](1).exclude(SortedSetIterator[Int]()).toList shouldBe List(1)
    SortedSetIterator[Int]().exclude(SortedSetIterator[Int](1)).toList shouldBe List()
    SortedSetIterator(1).exclude(SortedSetIterator(1)).toList shouldBe List()
    SortedSetIterator(1, 2, 3, 4).exclude(SortedSetIterator(3)).toList shouldBe List(1, 2, 4)
    SortedSetIterator(1, 2, 3, 4).exclude(SortedSetIterator(2, 3)).toList shouldBe List(1, 4)
    SortedSetIterator(1, 2, 3, 4).exclude(SortedSetIterator(1, 2, 3)).toList shouldBe List(4)
    SortedSetIterator(1, 2, 3, 4).exclude(SortedSetIterator(2, 3, 4)).toList shouldBe List(1)
    SortedSetIterator(0).exclude(SortedSetIterator(-1, 0)).toList shouldBe List()
  }

  it should "exclude any sorted iterator" in {
    forAll { (xs: List[Long], ys: List[Long]) =>
      val xit = xs.sorted.toIterator
      val yit = ys.sorted.toIterator

      val expected = (xs.toSet -- ys.toSet).toList.sorted
      SortedSetIterator(xit).exclude(SortedSetIterator(yit)).toList shouldBe expected
    }
  }

  it should "fetch specified elements from iterator" in {
    val it = SortedSetIterator(1, 2, 3, 4, 5).prefetch(3)
    it.fetched shouldBe Array(1, 2, 3)
    it.isAllFetched shouldBe false
  }

  it should "fetch all from iterator when size of iterator less then specified for prefetch" in {
    val it = SortedSetIterator(1).prefetch(3)
    it.fetched shouldBe Array(1)
    it.isAllFetched shouldBe true
  }

  it should "iterate all elements for partially fetched elements" in {
    val it = SortedSetIterator(1, 2, 3, 4, 5, 6).prefetch(3)
    it.toList shouldBe List(1, 2, 3, 4, 5, 6)
    it.hasNext shouldBe false
  }

  it should "iterate all elements for fully fetched elements" in {
    val it = SortedSetIterator(1, 2, 3, 4, 5, 6).prefetch(7)
    it.toList shouldBe List(1, 2, 3, 4, 5, 6)
    it.hasNext shouldBe false
  }
}
