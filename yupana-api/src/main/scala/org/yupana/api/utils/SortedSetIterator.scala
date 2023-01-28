/*
 * Copyright 2019 Rusexpertiza LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yupana.api.utils

import scala.reflect.ClassTag

abstract class SortedSetIterator[A: DimOrdering] extends Iterator[A] {

  def union(that: SortedSetIterator[A]): SortedSetIterator[A] = {
    if (this.isEmpty) that
    else if (that.isEmpty) this
    else new SortedSetIteratorImpl(new UnionSortedIteratorImpl(Seq(this, that)))
  }

  def intersect(that: SortedSetIterator[A]): SortedSetIterator[A] = {
    if (this.isEmpty) this
    else if (that.isEmpty) that
    else new IntersectSortedIteratorImpl(Seq(this, that))
  }

  def exclude(sub: SortedSetIterator[A]): SortedSetIterator[A] = {
    if (this.isEmpty || sub.isEmpty) this
    else new ExcludeSortedIteratorImpl(this, sub)
  }

  def prefetch(prefetchElements: Int)(implicit ct: ClassTag[A]): PrefetchedSortedSetIterator[A] = {
    new PrefetchedSortedSetIterator[A](this, prefetchElements)
  }
}

object SortedSetIterator {

  def empty[A: DimOrdering]: SortedSetIterator[A] = {
    new SortedSetIteratorImpl(new SingleSortedIteratorImpl[A](Iterator.empty))
  }

  def apply[A: DimOrdering](seq: A*): SortedSetIterator[A] = {
    new SortedSetIteratorImpl(new SingleSortedIteratorImpl[A](seq.iterator))
  }

  def apply[A: DimOrdering](it: Iterator[A]): SortedSetIterator[A] = {
    new SortedSetIteratorImpl(new SingleSortedIteratorImpl[A](it))
  }

  def intersectAll[A: DimOrdering](ids: Seq[SortedSetIterator[A]]): SortedSetIterator[A] = {
    if (ids.nonEmpty) new IntersectSortedIteratorImpl[A](ids) else SortedSetIterator.empty
  }

  def unionAll[A: DimOrdering](its: Seq[SortedSetIterator[A]]): SortedSetIterator[A] = {
    if (its.nonEmpty) new UnionSortedIteratorImpl[A](its) else SortedSetIterator.empty
  }
}

private class SingleSortedIteratorImpl[A](it: Iterator[A])(implicit ord: DimOrdering[A]) extends SortedSetIterator[A] {

  var prevValue: A = _

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

private class SortedSetIteratorImpl[A: DimOrdering](it: SortedSetIterator[A]) extends SortedSetIterator[A] {

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

      while ({
        if (it.hasNext) {
          t = it.next()
          hasT = true
        } else {
          hasT = false
        }
        hasT && t == nextVal
      }) ()
      nextVal = t
      hasNextVal = hasT
      hasT
    } else {
      true
    }
  }

  override def next(): A = {
    if (!hasNext) throw new NoSuchElementException("Next on empty iterator")
    hasNextVal = false
    nextVal
  }
}

private class UnionSortedIteratorImpl[A](its: Seq[SortedSetIterator[A]])(implicit ord: DimOrdering[A])
    extends SortedSetIterator[A] {

  private val bIts = its.map(_.buffered)

  override def hasNext: Boolean = {
    bIts.exists(_.hasNext)
  }

  override def next(): A = {
    var minIt = bIts.head

    for (bit <- bIts) {
      if (bit.hasNext && minIt.hasNext) {
        if (ord.lt(bit.head, minIt.head)) {
          minIt = bit
        }
      } else if (bit.hasNext) {
        minIt = bit
      }
    }

    minIt.next()
  }
}

private class IntersectSortedIteratorImpl[A](its: Seq[SortedSetIterator[A]])(implicit ord: DimOrdering[A])
    extends SortedSetIterator[A] {

  private val bIts = its.map(_.buffered)

  override def hasNext: Boolean = {
    bIts.forall(_.hasNext) && seekToNextEq()
  }

  override def next(): A = {
    if (hasNext) {
      bIts.tail.foreach(_.next())
      bIts.head.next()
    } else {
      throw new NoSuchElementException("Next on empty iterator")
    }
  }

  private def seekToNextEq(): Boolean = {

    while ({
      val maxHead = bIts.map(_.head).reduce(ord.max)

      bIts.foreach { bit =>
        if (bit.hasNext && ord.lt(bit.head, maxHead)) {
          bit.next()
        }
      }
      bIts.forall(_.hasNext) && bIts.exists(_.head != bIts.head.head)
    }) ()

    bIts.forall(bit => bit.hasNext && bit.head == bIts.head.head)
  }
}

private class ExcludeSortedIteratorImpl[A](it: SortedSetIterator[A], sub: SortedSetIterator[A])(
    implicit ord: DimOrdering[A]
) extends SortedSetIterator[A] {

  private val bIt = it.buffered
  private val bSub = sub.buffered

  override def hasNext: Boolean = {

    while ({
      while (bIt.hasNext && bSub.hasNext && ord.lt(bSub.head, bIt.head)) {
        bSub.next()
      }

      if (bIt.hasNext && bSub.hasNext && bIt.head == bSub.head) {
        bIt.next()
      }
      bIt.hasNext && bSub.hasNext && ord.lte(bSub.head, bIt.head)
    }) ()

    bIt.hasNext && !(bSub.hasNext && bIt.head == bSub.head)
  }

  override def next(): A = {
    if (hasNext) {
      bIt.next()
    } else {
      throw new NoSuchElementException("Next on empty iterator")
    }
  }
}

class PrefetchedSortedSetIterator[T] private[utils] (it: SortedSetIterator[T], prefetchElements: Int)(
    implicit val ord: DimOrdering[T],
    ct: ClassTag[T]
) extends SortedSetIterator[T] {

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
