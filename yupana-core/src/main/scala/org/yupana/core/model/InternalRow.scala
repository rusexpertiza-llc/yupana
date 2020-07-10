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

import org.yupana.api.{ HexString, Time }
import org.yupana.api.query.{ DimensionExpr, DimensionIdExpr, Expression, MetricExpr, TimeExpr }
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

  val timeIndex: Int = exprIndex.getOrElse(TimeExpr, -1)

  private val tagExprsIndexes: Array[Int] = table match {
    case Some(table) =>
      val tagIndexes = Array.fill[Int](Table.MAX_TAGS)(-1)

      exprIndex.toSeq.foreach {
        case (expr, index) =>
          val tag = expr match {
            case MetricExpr(metric) =>
              Some(metric.tag)

            case DimensionExpr(dimension: Dimension) =>
              Some(table.dimensionTag(dimension))

            case _ => None
          }
          tag.foreach { t =>
            tagIndexes(t & 0xFF) = index
          }
      }

      tagIndexes
    case None => Array.empty
  }

  private val dimIdIndex: Array[Int] = table match {
    case Some(table) =>
      val tagIndex = Array.fill[Int](Table.MAX_TAGS)(-1)

      exprIndex.toSeq.foreach {
        case (DimensionIdExpr(dimension: Dimension), index) =>
          val t = table.dimensionTag(dimension)
          tagIndex(t & 0xFF) = index

        case _ =>
      }

      tagIndex

    case None =>
      Array.empty
  }

  def this(queryContext: QueryContext) = this(queryContext.exprsIndex, queryContext.query.table)

  def set(tag: Int, v: Option[Any]): Unit = {
    val index = tagExprsIndexes(tag & 0xFF)
    if (index != -1) {
      data(index) = v
    }
  }

  def setId(tag: Int, v: Option[HexString]): Unit = {
    val index = dimIdIndex(tag & 0xFF)
    if (index != -1) {
      data(index) = v
    }
  }

  def needId(tag: Int): Boolean = dimIdIndex(tag & 0xFF) != -1

  def set(time: Option[Time]): Unit = {
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
