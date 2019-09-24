package org.yupana.api.utils

import scala.reflect.ClassTag

abstract class SortedSetIterator[A:Ordering] extends Iterator[A] {

  def union(that: SortedSetIterator[A]): SortedSetIterator[A] = {
    new SortedSetIteratorImpl(new UnionSortedIteratorImpl(this, that))
  }

  def intersect(that: SortedSetIterator[A]): SortedSetIterator[A] = {
    new IntersectSortedIteratorImpl(this, that)
  }

  def exclude(sub: SortedSetIterator[A]): SortedSetIterator[A] = {
    new ExcludeSortedIteratorImpl(this, sub)
  }

  def prefetch(prefetchElements: Int)(implicit ct: ClassTag[A]): PrefetchedSortedSetIterator[A] = {
    new PrefetchedSortedSetIterator[A](this, prefetchElements)
  }
}

object SortedSetIterator {

  def empty[A:Ordering]: SortedSetIterator[A] = {
    new SortedSetIteratorImpl(new SingleSortedIteratorImpl[A](Iterator.empty))
  }

  def apply[A: Ordering](seq: A*): SortedSetIterator[A] = {
    new SortedSetIteratorImpl(new SingleSortedIteratorImpl[A](seq.toIterator))
  }

  def apply[A: Ordering](it: Iterator[A]): SortedSetIterator[A] = {
    new SortedSetIteratorImpl(new SingleSortedIteratorImpl[A](it))
  }

  def apply[A: Ordering](): SortedSetIterator[A] = {
    new SortedSetIteratorImpl(new SingleSortedIteratorImpl(Iterator.empty))
  }

  def intersectAll[A: Ordering](ids: Iterator[SortedSetIterator[A]]): SortedSetIterator[A] = {
    if (ids.nonEmpty) ids.reduce(_ intersect _) else SortedSetIterator.empty
  }

  def unionAll[A: Ordering](its: Iterator[SortedSetIterator[A]]): SortedSetIterator[A] = {
    if (its.nonEmpty) its.reduce(_ union  _) else SortedSetIterator.empty
  }
}

private class SingleSortedIteratorImpl[A](it: Iterator[A])(implicit ord: Ordering[A]) extends SortedSetIterator[A] {

  var prevValue:A = _

  override def hasNext: Boolean = {
    it.hasNext
  }

  override def next(): A = {
    val cur = it.next()

    if (prevValue != null && ord.gt(prevValue, cur)) {
      throw new IllegalStateException(s"Previous value greater then current value $prevValue > $cur")
    } else {
      prevValue = cur
      cur
    }
  }
}

private class SortedSetIteratorImpl[A:Ordering](it: SortedSetIterator[A]) extends SortedSetIterator[A] {

  var nextVal: A = _
  var hasNextVal = false
  var isHead = true

  override def hasNext: Boolean = {
    if (isHead) {
      if (it.hasNext) {
        nextVal = it.next()
        hasNextVal = true
        isHead = false
      }

      hasNextVal
    } else if (!hasNextVal) {

      var t: A = nextVal
      var hasT = false

      do {
        if (it.hasNext) {
          t = it.next()
          hasT = true
        } else {
          hasT = false
        }
      } while (hasT && t == nextVal)
      nextVal = t
      hasNextVal= hasT
      hasT
    } else {
      true
    }
  }

  override def next(): A = {
    if (!hasNext) throw new IllegalStateException("Next on empty iterator")
    hasNextVal = false
    nextVal
  }
}

private class UnionSortedIteratorImpl[A](it1: SortedSetIterator[A], it2: SortedSetIterator[A])(implicit ord: Ordering[A]) extends SortedSetIterator[A] {

  private val bIt1 = it1.buffered
  private val bIt2 = it2.buffered

  override def hasNext: Boolean = {
    bIt1.hasNext || bIt2.hasNext
  }

  override def next(): A = {
    if (bIt1.hasNext && bIt2.hasNext) {
      if (ord.lt(bIt1.head, bIt2.head)) {
        bIt1.next()
      } else {
        bIt2.next()
      }
    } else if (bIt1.hasNext) {
      bIt1.next()
    } else {
      bIt2.next()
    }
  }
}

private class IntersectSortedIteratorImpl[A](it1: SortedSetIterator[A], it2: SortedSetIterator[A])(implicit ord: Ordering[A])  extends SortedSetIterator[A] {

  private val bIt1 = new SortedSetIteratorImpl(it1).buffered
  private val bIt2 = new SortedSetIteratorImpl(it2).buffered

  override def hasNext: Boolean = {
    bIt1.hasNext && bIt2.hasNext && seekToNextEq()
  }

  override def next(): A = {
    if (hasNext) {
      bIt1.next()
      bIt2.next()
    } else {
      throw new IllegalStateException("Next on empty iterator")
    }
  }

  private def seekToNextEq() = {
    import ord.mkOrderingOps

    do {
      val maxHead = ord.max(bIt1.head, bIt2.head)

      if (bIt1.hasNext && bIt1.head < maxHead) {
        bIt1.next()
      }

      if (bIt2.hasNext && bIt2.head < maxHead) {
        bIt2.next()
      }
    } while (bIt1.hasNext && bIt2.hasNext && bIt1.head != bIt2.head)

    bIt1.hasNext && bIt2.hasNext && bIt1.head == bIt2.head
  }
}


private class ExcludeSortedIteratorImpl[A](it: SortedSetIterator[A], sub: SortedSetIterator[A])(implicit ord: Ordering[A])  extends SortedSetIterator[A] {

  private val bIt = it.buffered
  private val bSub = sub.buffered

  override def hasNext: Boolean = {
    import ord.mkOrderingOps

    do {
      while (bIt.hasNext && bSub.hasNext && bSub.head < bIt.head) {
        bSub.next()
      }

      if (bIt.hasNext && bSub.hasNext && bIt.head == bSub.head) {
        bIt.next
      }
    } while (bIt.hasNext && bSub.hasNext && bSub.head <= bIt.head)

    bIt.hasNext && !(bSub.hasNext && bIt.head == bSub.head)
  }

  override def next(): A = {
    if (hasNext) {
      bIt.next()
    } else {
      throw new IllegalStateException("Next on empty iterator")
    }
  }
}

class PrefetchedSortedSetIterator[T] private[utils] (it: SortedSetIterator[T], prefetchElements: Int)(implicit val ord: Ordering[T], ct: ClassTag[T]) extends SortedSetIterator[T] {

  private val buffer = Array.ofDim[T](prefetchElements)
  private var bufferLen = 0
  private var index = 0

  def isAllFetched: Boolean = {
    hasNext
    !it.hasNext
  }

  def fetched: Array[T] = {
    prefetch()
    buffer.take(bufferLen)
  }

  override def hasNext: Boolean = {
    prefetch()
    index < bufferLen || it.hasNext
  }

  private def prefetch(): Unit = {
    if (bufferLen == 0) {
      while (bufferLen < prefetchElements && it.hasNext) {
        buffer(bufferLen) = it.next()
        bufferLen += 1
      }
    }
  }

  override def next(): T = {
    prefetch()
    val v = if (index < bufferLen) {
      buffer(index)
    } else {
      it.next()
    }
    index += 1
    v
  }
}
