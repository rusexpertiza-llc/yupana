package org.yupana.core
import org.yupana.core.model.BatchDataset

import scala.collection.AbstractIterator

class LimitIterator(it: Iterator[BatchDataset], n: Int) extends AbstractIterator[BatchDataset] {
  var count = 0
  override def hasNext: Boolean = count < n && it.hasNext

  override def next(): BatchDataset = {
    val nextBatch = it.next()
    var i = 0
    while (i < nextBatch.size) {
      val isDeletedBefore = nextBatch.isDeleted(i)
      if (count >= n) nextBatch.setDeleted(i)
      i += 1
      if (!isDeletedBefore) count += 1
    }
    nextBatch
  }
}