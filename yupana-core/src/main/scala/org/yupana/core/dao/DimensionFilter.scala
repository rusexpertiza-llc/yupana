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

package org.yupana.core.dao

import org.yupana.api.schema.Dimension

sealed trait DimensionFilter[T] {
  def and(tagFilter: DimensionFilter[T]): DimensionFilter[T]
  def or(tagFilter: DimensionFilter[T]): DimensionFilter[T]
  def exclude(dimFilter: DimensionFilter[T]): DimensionFilter[T]
  def map[U](f: (Dimension, Set[T]) => (Dimension, Set[U])): DimensionFilter[U]
  def toMap: Map[Dimension, Set[T]]
}

case class NoResult[T]() extends DimensionFilter[T] {
  override def and(tagFilter: DimensionFilter[T]): DimensionFilter[T] = NoResult()
  override def or(tagFilter: DimensionFilter[T]): DimensionFilter[T] = tagFilter
  override def exclude(tagFilter: DimensionFilter[T]): DimensionFilter[T] = NoResult()
  override def map[U](f: (Dimension, Set[T]) => (Dimension, Set[U])): DimensionFilter[U] = NoResult()
  override def toMap: Map[Dimension, Set[T]] = Map.empty
}

case class EmptyFilter[T]() extends DimensionFilter[T] {
  override def and(tagFilter: DimensionFilter[T]): DimensionFilter[T] = tagFilter
  override def or(tagFilter: DimensionFilter[T]): DimensionFilter[T] = EmptyFilter()
  override def exclude(tagFilter: DimensionFilter[T]): DimensionFilter[T] = EmptyFilter()
  override def map[U](f: (Dimension, Set[T]) => (Dimension, Set[U])): DimensionFilter[U] = EmptyFilter()
  override def toMap: Map[Dimension, Set[T]] = Map.empty
}

case class FiltersCollection[T](conditions: Map[Dimension, Set[T]]) extends DimensionFilter[T] {

  override def and(tagFilter: DimensionFilter[T]): DimensionFilter[T] = tagFilter match {
    case NoResult()    => NoResult()
    case EmptyFilter() => this
    case FiltersCollection(cs) =>
      val newConditions = conditions ++ cs.map {
        case (tag, ids) =>
          tag -> conditions.get(tag).map(_ intersect ids).getOrElse(ids)
      }

      DimensionFilter[T](newConditions)
  }

  override def or(tagFilter: DimensionFilter[T]): DimensionFilter[T] = tagFilter match {
    case NoResult()    => this
    case EmptyFilter() => EmptyFilter()
    case FiltersCollection(cs) =>
      val newConditions = conditions ++ cs.map {
        case (tag, ids) =>
          tag -> conditions.get(tag).map(_ union ids).getOrElse(ids)
      }

      DimensionFilter[T](newConditions)
  }

  override def exclude(tagFilter: DimensionFilter[T]): DimensionFilter[T] = tagFilter match {
    case NoResult() | EmptyFilter() => this
    case FiltersCollection(cs) =>
      val newConditions = conditions.map {
        case (tag, ids) =>
          tag -> (ids -- cs.getOrElse(tag, Set.empty))
      }

      DimensionFilter[T](newConditions)
  }

  override def map[U](f: (Dimension, Set[T]) => (Dimension, Set[U])): DimensionFilter[U] =
    DimensionFilter(conditions.map(f.tupled))

  override def toMap: Map[Dimension, Set[T]] = conditions
}

object DimensionFilter {
  def apply[T](dimIds: Map[Dimension, Set[T]]): DimensionFilter[T] = {
    if (dimIds.isEmpty) EmptyFilter[T]() else if (dimIds.exists(_._2.isEmpty)) NoResult() else FiltersCollection(dimIds)
  }
}
