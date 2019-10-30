package org.yupana.core.utils

import scala.collection.AbstractIterator

class OnFinishIterator[T](it: Iterator[T], finishAction: () => Unit) extends AbstractIterator[T] {

  var hasEnded = false

  override def hasNext: Boolean = {
    val n = it.hasNext
    if (!n && !hasEnded) {
      hasEnded = true
      finishAction()
    }
    n
  }

  override def next(): T = it.next()

}
