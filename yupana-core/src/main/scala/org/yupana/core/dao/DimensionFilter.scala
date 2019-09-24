package org.yupana.core.dao

import org.yupana.api.schema.Dimension
import org.yupana.api.utils.SortedSetIterator

sealed trait DimensionFilter[T] {
  def and(tagFilter: DimensionFilter[T]): DimensionFilter[T]
  def or(tagFilter: DimensionFilter[T]): DimensionFilter[T]
  def exclude(dimFilter: DimensionFilter[T]): DimensionFilter[T]
  def map[U](f: (Dimension, SortedSetIterator[T]) => (Dimension, SortedSetIterator[U])): DimensionFilter[U]
  def toMap: Map[Dimension, SortedSetIterator[T]]
}

case class NoResult[T]() extends DimensionFilter[T] {
  override def and(tagFilter: DimensionFilter[T]): DimensionFilter[T] = NoResult()
  override def or(tagFilter: DimensionFilter[T]): DimensionFilter[T] = tagFilter
  override def exclude(tagFilter: DimensionFilter[T]): DimensionFilter[T] = NoResult()
  override def map[U](f: (Dimension, SortedSetIterator[T]) => (Dimension, SortedSetIterator[U])): DimensionFilter[U] = NoResult()
  override def toMap: Map[Dimension, SortedSetIterator[T]] = Map.empty
}

case class EmptyFilter[T]() extends DimensionFilter[T] {
  override def and(tagFilter: DimensionFilter[T]): DimensionFilter[T] = tagFilter
  override def or(tagFilter: DimensionFilter[T]): DimensionFilter[T] = EmptyFilter()
  override def exclude(tagFilter: DimensionFilter[T]): DimensionFilter[T] = EmptyFilter()
  override def map[U](f: (Dimension, SortedSetIterator[T]) => (Dimension, SortedSetIterator[U])): DimensionFilter[U] = EmptyFilter()
  override def toMap: Map[Dimension, SortedSetIterator[T]] = Map.empty
}

case class FiltersCollection[T](conditions: Map[Dimension, SortedSetIterator[T]]) extends DimensionFilter[T] {

  override def and(tagFilter: DimensionFilter[T]): DimensionFilter[T] = tagFilter match {
    case NoResult() => NoResult()
    case EmptyFilter() => this
    case FiltersCollection(cs) =>
      val newConditions = conditions ++ cs.map { case (tag, ids) =>
        tag -> conditions.get(tag).map(_ intersect ids).getOrElse(ids)
      }

      DimensionFilter[T](newConditions)
  }

  override def or(tagFilter: DimensionFilter[T]): DimensionFilter[T] = tagFilter match {
    case NoResult() => this
    case EmptyFilter() => EmptyFilter()
    case FiltersCollection(cs) =>
      val newConditions = conditions ++ cs.map { case (tag, ids) =>
        tag -> conditions.get(tag).map(_ union ids).getOrElse(ids)
      }

      DimensionFilter[T](newConditions)
  }

  override def exclude(tagFilter: DimensionFilter[T]): DimensionFilter[T] = tagFilter match {
    case NoResult() | EmptyFilter() => this
    case FiltersCollection(cs) =>
      val newConditions = conditions.map { case (tag, ids) =>
        tag -> cs.get(tag).map(s => ids exclude s).getOrElse(ids)
      }

      DimensionFilter[T](newConditions)
  }

  override def map[U](f: (Dimension, SortedSetIterator[T]) => (Dimension, SortedSetIterator[U])): DimensionFilter[U] = {
    val mapped = conditions.map(f.tupled)
    if (mapped.forall(_._2.hasNext)) FiltersCollection(mapped) else NoResult()
  }

  override def toMap: Map[Dimension, SortedSetIterator[T]] = conditions
}

object DimensionFilter {

  def empty[T]: EmptyFilter[T] = EmptyFilter()

  def apply[T](dimIds: Map[Dimension, SortedSetIterator[T]]): DimensionFilter[T] = {
    if (dimIds.isEmpty) {
      EmptyFilter[T]()
    } else if (dimIds.exists(_._2.isEmpty)) {
      NoResult()
    } else {
      FiltersCollection(dimIds)
    }
  }

  def apply[T](dim:Dimension, ids: SortedSetIterator[T]): DimensionFilter[T] = {
    new FiltersCollection[T](Map(dim -> ids))
  }

  def apply[T: Ordering](dim:Dimension, ids:Set[T]): DimensionFilter[T] = {
    new FiltersCollection[T](Map(dim -> SortedSetIterator(ids.toList.sorted.iterator)))
  }

  def apply[T: Ordering](dim: Dimension, id: T): DimensionFilter[T] = {
    new FiltersCollection[T](Map(dim -> SortedSetIterator(id)))
  }}
