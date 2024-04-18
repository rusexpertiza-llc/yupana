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

package org.yupana.core.jit

import org.yupana.api.Time
import org.yupana.api.query.{ LagExpr, WindowFunctionExpr }
import org.yupana.core.model.{ BatchDataset, HashTableDataset }
import org.yupana.serialization.MemoryBuffer

import java.time.OffsetDateTime
import java.time.temporal.{ ChronoUnit, TemporalAdjuster, TemporalUnit }
import scala.collection.AbstractIterator

trait ExpressionCalculator {

  def evaluateFilter(
      batch: BatchDataset
  ): Unit

  def evaluateExpressions(
      batch: BatchDataset
  ): Unit

  def createKey(batch: BatchDataset, rowNum: Int): AnyRef
  def evaluateFold(acc: HashTableDataset, batch: BatchDataset): Unit

  def evaluateCombine(acc: HashTableDataset, batch: BatchDataset): Unit

  def evaluatePostCombine(batch: BatchDataset): Unit

  def evaluatePostAggregateExprs(batch4: BatchDataset): Unit
  def evaluatePostFilter(batch: BatchDataset): Unit

  def evaluateReadRow(buf: MemoryBuffer, batch: BatchDataset, rowNum: Int): Unit

  def evaluateWindow[I, O](winFuncExpr: WindowFunctionExpr[I, O], values: Array[I], index: Int): O = {
    winFuncExpr match {
      case LagExpr(_) => if (index > 0) values(index - 1).asInstanceOf[O] else null.asInstanceOf[O]
    }
  }
}

object ExpressionCalculator {
  def truncateTime(adjuster: TemporalAdjuster)(time: Time): Time = {
    Time(time.toDateTime.`with`(adjuster).truncatedTo(ChronoUnit.DAYS))
  }

  def truncateTime(unit: TemporalUnit)(time: Time): Time = {
    Time(time.toDateTime.truncatedTo(unit))
  }

  def truncateTimeBy(f: OffsetDateTime => OffsetDateTime)(time: Time): Time = {
    Time(f(time.toDateTime).truncatedTo(ChronoUnit.DAYS))
  }

  def splitBy(s: String, p: Char => Boolean): Iterator[String] = new AbstractIterator[String] {
    private val len = s.length
    private var pos = 0

    override def hasNext: Boolean = pos < len

    override def next(): String = {
      if (pos >= len) throw new NoSuchElementException("next on empty iterator")
      val start = pos
      while (pos < len && !p(s(pos))) pos += 1
      val res = s.substring(start, pos min len)
      while (pos < len && p(s(pos))) pos += 1
      res
    }
  }
}
