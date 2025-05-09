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
