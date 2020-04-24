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

package org.yupana.core.model

import org.yupana.api.Time
import org.yupana.api.query.{ DimensionExpr, Expression, MetricExpr, TimeExpr }
import org.yupana.api.schema.{ Dimension, Table }
import org.yupana.core.QueryContext

class InternalRow(val data: Array[Option[Any]]) extends Serializable {

  def set(queryContext: QueryContext, expr: Expression, v: Option[Any]): InternalRow = {
    data(queryContext.exprsIndex(expr)) = v
    this
  }

  def set(exprIndex: scala.collection.Map[Expression, Int], expr: Expression, v: Option[Any]): InternalRow = {
    data(exprIndex(expr)) = v
    this
  }

  def set(index: Int, v: Option[Any]): InternalRow = {
    data(index) = v
    this
  }

  def get[T](queryContext: QueryContext, expr: Expression): Option[T] = {
    data(queryContext.exprsIndex(expr)).asInstanceOf[Option[T]]
  }

  def get[T](exprIndex: scala.collection.Map[Expression, Int], expr: Expression): Option[T] = {
    data(exprIndex(expr)).asInstanceOf[Option[T]]
  }

  def get[T](index: Int): Option[T] = {
    data(index).asInstanceOf[Option[T]]
  }

  def copy: InternalRow = {
    val dataCopy = Array.ofDim[Option[Any]](data.length)
    Array.copy(data, 0, dataCopy, 0, data.length)
    new InternalRow(dataCopy)
  }
}

class InternalRowBuilder(val exprIndex: scala.collection.Map[Expression, Int], table: Option[Table])
    extends Serializable {
  private val data = Array.fill(exprIndex.size)(Option.empty[Any])

  val timeIndex = exprIndex.getOrElse(TimeExpr, -1)

  private val tagExprsIndexes: Array[Array[Int]] = table match {
    case Some(table) =>
      val tagIndexes = exprIndex.toSeq.map {
        case (expr, index) =>
          expr match {
            case MetricExpr(metric) =>
              metric.tag -> index
            case DimensionExpr(dimension: Dimension) =>
              table.dimensionTag(dimension) -> index
            case _ => 0.toByte -> index
          }
      }
      val arr = Array.ofDim[Array[Int]](Table.MAX_TAGS)

      tagIndexes.groupBy(_._1).mapValues(_.map(_._2)).foreach {
        case (tag, indexes) =>
          arr(tag & 0xFF) = indexes.toArray
      }

      arr
    case None => Array.empty
  }

  def this(queryContext: QueryContext) = this(queryContext.exprsIndex, queryContext.query.table)

  def set(tag: Byte, v: Option[Any]) = {
    val indexes = tagExprsIndexes(tag & 0xFF)
    if (indexes != null) {
      indexes.foreach(idx => data(idx) = v)
    }
  }

  def set(time: Option[Time]) = {
    if (timeIndex != -1) data(timeIndex) = time
  }

  def set(expr: Expression, v: Option[Any]): InternalRowBuilder = {
    data(exprIndex(expr)) = v
    this
  }

  def buildAndReset(): InternalRow = {
    val dataCopy = Array.ofDim[Option[Any]](data.length)
    Array.copy(data, 0, dataCopy, 0, data.length)
    val result = new InternalRow(dataCopy)

    data.indices.foreach(i => data(i) = None)

    result
  }
}
