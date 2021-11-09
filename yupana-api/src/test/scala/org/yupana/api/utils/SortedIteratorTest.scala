package org.yupana.api.utils

import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SortedIteratorTest extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  implicit val ord: Ordering[Long] = Ordering.fromLessThan((a, b) => java.lang.Long.compareUnsigned(a, b) < 0)

  "SortedSetIterator" should "throw IllegalStateException when items not sorted" in {
    val it = SortedSetIterator[Long](1, 2, 2, 3, 4, 5)

    it.toList shouldBe List[Long](1, 2, 3, 4, 5)
  }

  it should "throws IllegalSTateException when items not sorted" in {
    val it = SortedSetIterator[Long](1, 2, 4, 3, 5)

    an[IllegalStateException] should be thrownBy it.sum
  }

  it should "remove duplicates" in {
    val it = SortedSetIterator[Long](1, 1, 1, 2, 2, 2, 3, 4, 5, 6, 6, 6, 6)

    it.toList shouldBe List[Long](1, 2, 3, 4, 5, 6)
  }

  it should "union iterators" in {
    SortedSetIterator[Long]().union(SortedSetIterator(1)).toList shouldBe List[Long](1)

    SortedSetIterator[Long](1).union(SortedSetIterator[Long](1)).toList shouldBe List[Long](1)
    SortedSetIterator[Long](1, 2, 3).union(SortedSetIterator[Long](1)).toList shouldBe List[Long](1, 2, 3)
    SortedSetIterator[Long](1).union(SortedSetIterator[Long](1, 2, 3, 4)).toList shouldBe List[Long](1, 2, 3, 4)
    SortedSetIterator[Long](1, 1, 1, 5, 5,
      5).union(SortedSetIterator[Long](1, 2, 4)).toList shouldBe List[Long](1, 2, 4, 5)
    SortedSetIterator[Long](1, 1, 1, 5, 5,
      5).union(SortedSetIterator[Long](1, 2, 4, 5, 5, 5)).toList shouldBe List[Long](1, 2, 4, 5)
  }

  it should "union any sorted iterators" in {

    forAll { (xs: List[Long], ys: List[Long]) =>
      val xit = xs.sorted.iterator
      val yit = ys.sorted.iterator

      val expected = (xs ++ ys).distinct.sorted
      SortedSetIterator[Long](xit).union(SortedSetIterator[Long](yit)).toList shouldBe expected
    }
  }

  it should "intersect iterators" in {
    SortedSetIterator[Long](0).intersect(SortedSetIterator[Long](-1)).toList shouldBe List()
    SortedSetIterator[Long]().intersect(SortedSetIterator[Long](1)).toList shouldBe List()
    SortedSetIterator[Long](1).intersect(SortedSetIterator[Long](1)).toList shouldBe List(1)
    SortedSetIterator[Long](1, 2, 3).intersect(SortedSetIterator(1)).toList shouldBe List(1)
    SortedSetIterator[Long](1).intersect(SortedSetIterator(1, 2, 3, 4)).toList shouldBe List(1)
    SortedSetIterator[Long](1, 1, 1, 4, 5, 5, 5).intersect(SortedSetIterator(1, 2, 4)).toList shouldBe List(1, 4)
    SortedSetIterator[Long](1, 1, 1, 5, 5, 5).intersect(SortedSetIterator(1, 2, 4, 5, 5, 5)).toList shouldBe List(1, 5)
  }

  it should "intersect any sorted iterators" in {
    forAll { (xs: List[Long], ys: List[Long]) =>
      val xit = xs.sorted.iterator
      val yit = ys.sorted.iterator

      val expected = xs.toSet.intersect(ys.toSet).toList.sorted
      SortedSetIterator[Long](xit).intersect(SortedSetIterator(yit)).toList shouldBe expected
    }
  }

  it should "exclude sorted iterator" in {
    SortedSetIterator[Long]().exclude(SortedSetIterator[Long]()).toList shouldBe List()
    SortedSetIterator[Long](1).exclude(SortedSetIterator[Long]()).toList shouldBe List(1)
    SortedSetIterator[Long]().exclude(SortedSetIterator[Long](1)).toList shouldBe List()
    SortedSetIterator[Long](1).exclude(SortedSetIterator(1)).toList shouldBe List()
    SortedSetIterator[Long](1, 2, 3, 4).exclude(SortedSetIterator(3)).toList shouldBe List(1, 2, 4)
    SortedSetIterator[Long](1, 2, 3, 4).exclude(SortedSetIterator(2, 3)).toList shouldBe List(1, 4)
    SortedSetIterator[Long](1, 2, 3, 4).exclude(SortedSetIterator(1, 2, 3)).toList shouldBe List(4)
    SortedSetIterator[Long](1, 2, 3, 4).exclude(SortedSetIterator(2, 3, 4)).toList shouldBe List(1)
    SortedSetIterator[Long](0).exclude(SortedSetIterator(0, -1)).toList shouldBe List()
  }

  it should "exclude any sorted iterator" in {
    forAll { (xs: List[Long], ys: List[Long]) =>
      val xit = xs.sorted.iterator
      val yit = ys.sorted.iterator

      val expected = (xs.toSet -- ys.toSet).toList.sorted
      SortedSetIterator(xit).exclude(SortedSetIterator(yit)).toList shouldBe expected
    }
  }

  it should "fetch specified elements from iterator" in {
    val it = SortedSetIterator[Long](1, 2, 3, 4, 5).prefetch(3)
    it.fetched shouldBe Array(1, 2, 3)
    it.isAllFetched shouldBe false
  }

  it should "fetch all from iterator when size of iterator less then specified for prefetch" in {
    val it = SortedSetIterator[Long](1).prefetch(3)
    it.fetched shouldBe Array(1)
    it.isAllFetched shouldBe true
  }

  it should "iterate all elements for partially fetched elements" in {
    val it = SortedSetIterator[Long](1, 2, 3, 4, 5, 6).prefetch(3)
    it.toList shouldBe List(1, 2, 3, 4, 5, 6)
    it.hasNext shouldBe false
  }

  it should "iterate all elements for fully fetched elements" in {
    val it = SortedSetIterator[Long](1, 2, 3, 4, 5, 6).prefetch(7)
    it.toList shouldBe List(1, 2, 3, 4, 5, 6)
    it.hasNext shouldBe false
  }

  it should "union a lot of iterators" in {
    val its = 1L to 500000 map (x => SortedSetIterator(Iterator(x)))
    val uit = SortedSetIterator.unionAll(its)
    uit.hasNext shouldBe true
  }
}
