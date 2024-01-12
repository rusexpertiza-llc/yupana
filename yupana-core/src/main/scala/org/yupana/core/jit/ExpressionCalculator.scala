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
import org.yupana.api.utils.Tokenizer
import org.yupana.core.model.{ InternalRow, InternalRowBuilder }
import org.yupana.readerwriter.MemoryBuffer

import java.time.OffsetDateTime
import java.time.temporal.{ ChronoUnit, TemporalAdjuster, TemporalUnit }
import scala.collection.AbstractIterator

trait ExpressionCalculator {

  def evaluateFilter(
      internalRowBuilder: InternalRowBuilder,
      tokenizer: Tokenizer,
      internalRow: InternalRow
  ): Boolean

  def evaluateExpressions(
      internalRowBuilder: InternalRowBuilder,
      tokenizer: Tokenizer,
      internalRow: InternalRow
  ): InternalRow

  def evaluateZero(
      internalRowBuilder: InternalRowBuilder,
      tokenizer: Tokenizer,
      internalRow: InternalRow
  ): InternalRow

  def evaluateSequence(
      internalRowBuilder: InternalRowBuilder,
      tokenizer: Tokenizer,
      accumulator: InternalRow,
      internalRow: InternalRow
  ): InternalRow

  def evaluateCombine(
      internalRowBuilder: InternalRowBuilder,
      tokenizer: Tokenizer,
      a: InternalRow,
      b: InternalRow
  ): InternalRow

  def evaluatePostMap(
      internalRowBuilder: InternalRowBuilder,
      tokenizer: Tokenizer,
      internalRow: InternalRow
  ): InternalRow

  def evaluatePostAggregateExprs(
      internalRowBuilder: InternalRowBuilder,
      tokenizer: Tokenizer,
      internalRow: InternalRow
  ): InternalRow

  def evaluatePostFilter(
      internalRowBuilder: InternalRowBuilder,
      tokenizer: Tokenizer,
      row: InternalRow
  ): Boolean

  def evaluateReadRow(
      row: MemoryBuffer,
      internalRowBuilder: InternalRowBuilder
  ): InternalRow

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
