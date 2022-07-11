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

import java.time.temporal.{ ChronoUnit, TemporalAdjuster, TemporalUnit }

import org.yupana.api.Time
import org.yupana.api.query._
import org.yupana.api.utils.Tokenizer
import org.yupana.core.model.InternalRow

import scala.collection.AbstractIterator

trait ExpressionCalculator {
  def evaluateFilter(tokenizer: Tokenizer, internalRow: InternalRow): Boolean
  def evaluateExpressions(tokenizer: Tokenizer, internalRow: InternalRow): InternalRow
  def evaluateFold(tokenizer: Tokenizer, accumulator: InternalRow, internalRow: InternalRow): InternalRow
  def evaluateMap(tokenizer: Tokenizer, internalRow: InternalRow): InternalRow
  def evaluateReduce(tokenizer: Tokenizer, a: InternalRow, b: InternalRow): InternalRow
  def evaluatePostMap(tokenizer: Tokenizer, internalRow: InternalRow): InternalRow
  def evaluatePostAggregateExprs(tokenizer: Tokenizer, internalRow: InternalRow): InternalRow
  def evaluatePostFilter(tokenizer: Tokenizer, row: InternalRow): Boolean

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
