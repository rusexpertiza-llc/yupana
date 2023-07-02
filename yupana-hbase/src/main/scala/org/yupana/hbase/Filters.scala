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

package org.yupana.hbase

import org.yupana.api.Time
import org.yupana.api.schema.{ DictionaryDimension, Dimension, HashDimension, RawDimension }
import org.yupana.api.utils.SortedSetIterator
import org.yupana.core.utils.CollectionUtils

class Filters(
    includes: Map[Dimension, SortedSetIterator[_]],
    excludes: Map[Dimension, SortedSetIterator[_]],
    val includeTime: Option[Set[Time]],
    val excludeTime: Option[Set[Time]]
) {

  def allIncludes: Map[Dimension, SortedSetIterator[_]] = {
    includes.map {
      case (d, i) =>
        val s = excludes.get(d) match {
          case Some(e) => i.asInstanceOf[SortedSetIterator[d.R]] exclude e.asInstanceOf[SortedSetIterator[d.R]]
          case None    => i
        }
        d -> s
    }
  }

  def allExcludes: Map[Dimension, SortedSetIterator[_]] = excludes
}

object Filters {

  type ValuesItIds = (DictionaryDimension, SortedSetIterator[String]) => SortedSetIterator[Long]
  def newBuilder(valuesToIds: ValuesItIds): Builder =
    new Builder(valuesToIds, Map.empty, Map.empty, Map.empty, Map.empty, Option.empty, Option.empty)

  def empty = new Filters(Map.empty, Map.empty, None, None)

  class Builder(
      valuesToIds: (DictionaryDimension, SortedSetIterator[String]) => SortedSetIterator[Long],
      private val incValues: Map[Dimension, SortedSetIterator[_]],
      private val excValues: Map[Dimension, SortedSetIterator[_]],
      private val incIds: Map[Dimension, SortedSetIterator[_]],
      private val excIds: Map[Dimension, SortedSetIterator[_]],
      private val incTime: Option[Set[Time]],
      private val excTime: Option[Set[Time]]
  ) {
    def getIncValues[T](dimension: Dimension.Aux2[T, _]): Option[SortedSetIterator[T]] = {
      incValues.get(dimension).asInstanceOf[Option[SortedSetIterator[dimension.T]]]
    }

    def getIncIds[R](dimension: Dimension.Aux2[_, R]): Option[SortedSetIterator[R]] = {
      incIds.get(dimension).asInstanceOf[Option[SortedSetIterator[dimension.R]]]
    }

    def getExcValues[T](dimension: Dimension.Aux2[T, _]): Option[SortedSetIterator[T]] = {
      excValues.get(dimension).asInstanceOf[Option[SortedSetIterator[dimension.T]]]
    }

    def getExcIds[R](dimension: Dimension.Aux2[_, R]): Option[SortedSetIterator[R]] = {
      excIds.get(dimension).asInstanceOf[Option[SortedSetIterator[dimension.R]]]
    }

    def includeValues[T](dim: Dimension.Aux2[T, _], vs: SortedSetIterator[T]): Builder = {
      val newValues = intersect(getIncValues(dim), vs)
      new Builder(valuesToIds, incValues + (dim -> newValues), excValues, incIds, excIds, incTime, excTime)
    }

    def excludeValues[T](dim: Dimension.Aux2[T, _], vs: SortedSetIterator[T]): Builder = {
      val newValues = union(getExcValues(dim), vs)
      new Builder(valuesToIds, incValues, excValues + (dim -> newValues), incIds, excIds, incTime, excTime)
    }

    def includeIds[R](dim: Dimension.Aux2[_, R], is: SortedSetIterator[R]): Builder = {
      val newIds = intersect(getIncIds(dim), is)
      new Builder(valuesToIds, incValues, excValues, incIds + (dim -> newIds), excIds, incTime, excTime)
    }

    def excludeIds[R](dim: Dimension.Aux2[_, R], is: SortedSetIterator[R]): Builder = {
      val newIds = union(getExcIds(dim), is)

      new Builder(valuesToIds, incValues, excValues, incIds, excIds + (dim -> newIds), incTime, excTime)
    }

    def includeTime(times: Set[Time]): Builder = {
      val newTime = incTime.map(_ intersect times).getOrElse(times)
      new Builder(valuesToIds, incValues, excValues, incIds, excIds, Some(newTime), excTime)
    }

    def excludeTime(times: Set[Time]): Builder = {
      val newTime = excTime.map(_ union times).getOrElse(times)
      new Builder(valuesToIds, incValues, excValues, incIds, excIds, incTime, Some(newTime))
    }

    def includeValue[T](dim: Dimension.Aux2[T, _], v: T): Builder = {
      includeValues(dim, SortedSetIterator(v)(dim.tOrdering))
    }

    def includeValues[T](dim: Dimension.Aux2[T, _], vs: Set[T]): Builder = {
      includeValues(dim, SortedSetIterator(vs.toList.sortWith(dim.tOrdering.lt).iterator)(dim.tOrdering))
    }

    def excludeValue[T](dim: Dimension.Aux2[T, _], v: T): Builder = {
      excludeValues(dim, SortedSetIterator(v)(dim.tOrdering))
    }

    def excludeValues[T](dim: Dimension.Aux2[T, _], vs: Set[T]): Builder = {
      excludeValues(dim, SortedSetIterator(vs.toList.sortWith(dim.tOrdering.lt).iterator)(dim.tOrdering))
    }

    def includeTime(t: Time): Builder = {
      includeTime(Set(t))
    }

    def excludeTime(t: Time): Builder = {
      excludeTime(Set(t))
    }

    def includeIds[R](dim: Dimension.Aux2[_, R], ids: Seq[R]): Builder = {
      includeIds(dim, SortedSetIterator(ids.sortWith(dim.rOrdering.lt).iterator)(dim.rOrdering))
    }

    def excludeIds[R](dim: Dimension.Aux2[_, R], ids: Seq[R]): Builder = {
      excludeIds(dim, SortedSetIterator(ids.sortWith(dim.rOrdering.lt).iterator)(dim.rOrdering))
    }

    def union(that: Builder): Builder = {

      def union(a: SortedSetIterator[_], b: SortedSetIterator[_]): SortedSetIterator[_] = {
        a match {
          case x: SortedSetIterator[t] => x.union(b.asInstanceOf[SortedSetIterator[t]])
        }
      }

      def intersect(a: SortedSetIterator[_], b: SortedSetIterator[_]): SortedSetIterator[_] = {
        a match {
          case x: SortedSetIterator[t] => x.intersect(b.asInstanceOf[SortedSetIterator[t]])
        }
      }

      val a = this.convertValues
      val b = that.convertValues

      new Builder(
        valuesToIds,
        CollectionUtils.mergeMaps[Dimension, SortedSetIterator[_]](a.incValues, b.incValues, union),
        CollectionUtils.mergeMaps(a.excValues, b.excValues, intersect),
        CollectionUtils.mergeMaps[Dimension, SortedSetIterator[_]](a.incIds, b.incIds, union),
        CollectionUtils.mergeMaps(a.excIds, b.excIds, intersect),
        unionOptSets(a.incTime, b.incTime),
        intersectOptSets(a.excTime, b.excTime)
      )
    }

    private def hashValues[T, R](d: HashDimension[T, R], values: SortedSetIterator[T]): SortedSetIterator[R] = {
      SortedSetIterator(values.map(d.hashFunction).toSeq.sortWith(d.rOrdering.lt).iterator)(d.rOrdering)
    }

    private def intersectOptIterators[T](
        ids1: Option[SortedSetIterator[T]],
        ids2: Option[SortedSetIterator[T]]
    ): Option[SortedSetIterator[T]] = {

      (ids1, ids2) match {
        case (Some(i1), Some(i2)) => Some(i1 intersect i2)
        case (Some(i1), None)     => Some(i1)
        case (None, Some(i2))     => Some(i2)
        case (None, None)         => None
      }
    }

    private def intersectOptSets[T](ids1: Option[Set[T]], ids2: Option[Set[T]]): Option[Set[T]] = {
      (ids1, ids2) match {
        case (Some(i1), Some(i2)) => Some(i1 intersect i2)
        case (Some(i1), None)     => Some(i1)
        case (None, Some(i2))     => Some(i2)
        case (None, None)         => None
      }
    }

    def includeFilter: Map[Dimension, SortedSetIterator[_]] = {
      val dims = incValues.keySet ++ incIds.keySet

      dims.flatMap {
        case d: DictionaryDimension =>
          val valueIds = getIncValues(d).map(vs => valuesToIds(d, vs))
          val ids = getIncIds(d)
          intersectOptIterators(valueIds, ids).map(d -> _).asInstanceOf[Option[(Dimension, SortedSetIterator[_])]]

        case r: RawDimension[_] =>
          intersectOptIterators(getIncIds(r), getIncValues(r)).map(vs => r -> vs)

        case h: HashDimension[_, _] =>
          val valueIds = getIncValues[h.T](h).map(vs => hashValues[h.T, h.R](h, vs))
          val ids = getIncIds[h.R](h)
          intersectOptIterators(valueIds, ids).map(h -> _).asInstanceOf[Option[(Dimension, SortedSetIterator[_])]]
      }.toMap
    }

    private def unionOptIterators[T](
        ids1: Option[SortedSetIterator[T]],
        ids2: Option[SortedSetIterator[T]]
    ): Option[SortedSetIterator[T]] = {

      (ids1, ids2) match {
        case (Some(i1), Some(i2)) => Some(i1 union i2)
        case (Some(i1), None)     => Some(i1)
        case (None, Some(i2))     => Some(i2)
        case (None, None)         => None
      }
    }

    private def unionOptSets[T](ids1: Option[Set[T]], ids2: Option[Set[T]]): Option[Set[T]] = {
      (ids1, ids2) match {
        case (Some(i1), Some(i2)) => Some(i1 union i2)
        case (Some(i1), None)     => Some(i1)
        case (None, Some(i2))     => Some(i2)
        case (None, None)         => None
      }
    }

    def excludeFilter: Map[Dimension, SortedSetIterator[_]] = {
      val dims = excValues.keySet ++ excIds.keySet
      dims.flatMap {
        case d: DictionaryDimension =>
          val valueIds = getExcValues(d).map(vs => valuesToIds(d, vs))
          val ids = getExcIds(d)
          unionOptIterators(valueIds, ids).map(d -> _).asInstanceOf[Option[(Dimension, SortedSetIterator[_])]]

        case r: RawDimension[_] =>
          unionOptIterators(getExcIds(r), getExcValues(r)).map(vs => r -> vs)

        case h: HashDimension[_, _] =>
          val valueIds = getExcValues(h).map(vs => hashValues(h, vs))
          val ids = getExcIds(h)
          unionOptIterators(valueIds, ids).map(h -> _).asInstanceOf[Option[(Dimension, SortedSetIterator[_])]]
      }.toMap
    }

    private def convertValues: Builder = {
      new Builder(valuesToIds, Map.empty, Map.empty, includeFilter, excludeFilter, incTime, excTime)
    }

    def build: Filters = {
      new Filters(includeFilter, excludeFilter, incTime, excTime)
    }

    private def intersect[T](cur: Option[SortedSetIterator[T]], vs: SortedSetIterator[T]): SortedSetIterator[T] = {
      cur match {
        case Some(it) => it intersect vs
        case None     => vs
      }
    }

    private def union[T](cur: Option[SortedSetIterator[T]], vs: SortedSetIterator[T]): SortedSetIterator[T] = {
      cur match {
        case Some(it) => it union vs
        case None     => vs
      }
    }
  }
}
