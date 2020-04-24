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
import org.yupana.api.schema.{ DictionaryDimension, Dimension, RawDimension }
import org.yupana.api.utils.{ DimOrdering, SortedSetIterator }

class Filters(
    includes: Map[Dimension, SortedSetIterator[_]],
    excludes: Map[Dimension, SortedSetIterator[_]],
    val includeTime: Option[SortedSetIterator[Time]],
    val excludeTime: Option[SortedSetIterator[Time]]
) {
  def getIncludes[R](dim: Dimension.Aux2[_, R]): Option[SortedSetIterator[R]] =
    includes.get(dim).asInstanceOf[Option[SortedSetIterator[R]]]

  def getExcludes[R](dim: Dimension.Aux2[_, R]): Option[SortedSetIterator[R]] =
    excludes.get(dim).asInstanceOf[Option[SortedSetIterator[R]]]

  def allIncludes: Map[Dimension, SortedSetIterator[_]] = {
    includes.map {
      case (d, i) =>
        val s = getExcludes(d.asInstanceOf[Dimension.Aux2[d.T, d.R]]) match {
          case Some(e) => i.asInstanceOf[SortedSetIterator[d.R]] exclude e
          case None    => i
        }
        d -> s
    }.toMap
  }

  def allExcludes: Map[Dimension, SortedSetIterator[_]] = excludes
}

object Filters {

  def newBuilder: Builder = new Builder(Map.empty, Map.empty, Map.empty, Map.empty, Option.empty, Option.empty)

  def empty = new Filters(Map.empty, Map.empty, None, None)

  class Builder(
      incValues: Map[Dimension, SortedSetIterator[_]],
      excValues: Map[Dimension, SortedSetIterator[_]],
      incIds: Map[Dimension, SortedSetIterator[_]],
      excIds: Map[Dimension, SortedSetIterator[_]],
      incTime: Option[SortedSetIterator[Time]],
      excTime: Option[SortedSetIterator[Time]]
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
      new Builder(incValues + (dim -> newValues), excValues, incIds, excIds, incTime, excTime)
    }

    def excludeValues[T](dim: Dimension.Aux2[T, _], vs: SortedSetIterator[T]): Builder = {
      val newValues = union(getExcValues(dim), vs)
      new Builder(incValues, excValues + (dim -> newValues), incIds, excIds, incTime, excTime)
    }

    def includeIds[R](dim: Dimension.Aux2[_, R], is: SortedSetIterator[R]): Builder = {
      val newIds = intersect(getIncIds(dim), is)
      new Builder(incValues, excValues, incIds + (dim -> newIds), excIds, incTime, excTime)
    }

    def excludeIds[R](dim: Dimension.Aux2[_, R], is: SortedSetIterator[R]): Builder = {
      val newIds = union(getIncIds(dim), is)

      new Builder(incValues, excValues, incIds, excIds + (dim -> newIds), incTime, excTime)
    }

    def includeTime(times: SortedSetIterator[Time]): Builder = {
      val newTime = intersect(incTime, times)
      new Builder(incValues, excValues, incIds, excIds, Some(newTime), excTime)
    }

    def excludeTime(times: SortedSetIterator[Time]): Builder = {
      val newTime = union(excTime, times)
      new Builder(incValues, excValues, incIds, excIds, incTime, Some(newTime))
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

    def includeIds[R](dim: Dimension.Aux2[_, R], is: Set[R]): Builder = {
      includeIds(dim, SortedSetIterator(is.toList.sortWith(dim.rOrdering.lt).iterator)(dim.rOrdering))
    }

    def excludeIds[R](dim: Dimension.Aux2[_, R], is: Set[R]): Builder = {
      excludeIds(dim, SortedSetIterator(is.toList.sortWith(dim.rOrdering.lt).iterator)(dim.rOrdering))
    }

    def includeTime(t: Time): Builder = {
      includeTime(SortedSetIterator(t))
    }

    def includeTime(t: Set[Time]): Builder = {
      includeTime(SortedSetIterator(t.toList.sortWith(implicitly[DimOrdering[Time]].lt).iterator))
    }

    def excludeTime(t: Time): Builder = {
      excludeTime(SortedSetIterator(t))
    }

    def excludeTime(t: Set[Time]): Builder = {
      excludeTime(SortedSetIterator(t.toList.sortWith(implicitly[DimOrdering[Time]].lt).iterator))
    }

    def includeFilter(
        valuesToIds: (DictionaryDimension, SortedSetIterator[String]) => SortedSetIterator[Long]
    ): Map[Dimension, SortedSetIterator[_]] = {
      val dims = incValues.keySet ++ incIds.keySet
      dims.flatMap {
        case d: DictionaryDimension =>
          val valueIds = getIncValues(d).map(vs => valuesToIds(d, vs))
          val ids = getIncIds(d)
          val r = (valueIds, ids) match {
            case (Some(v), Some(i)) => Some(d -> (v intersect i))
            case (Some(v), None)    => Some(d -> v)
            case (None, Some(i))    => Some(d -> i)
            case (None, None)       => None
          }

          r.asInstanceOf[Option[(Dimension, SortedSetIterator[_])]]

        case r: RawDimension[_] =>
          getIncValues(r).map(vs => r -> vs)
      }.toMap
    }

    def excludeFilter(
        valuesToIds: (DictionaryDimension, SortedSetIterator[String]) => SortedSetIterator[Long]
    ): Map[Dimension, SortedSetIterator[_]] = {
      val dims = excValues.keySet ++ excIds.keySet
      dims.flatMap {
        case d: DictionaryDimension =>
          val valueIds = getExcValues(d).map(vs => valuesToIds(d, vs))
          val ids = getExcIds(d)
          val r = (valueIds, ids) match {
            case (Some(v), Some(i)) => Some(d -> (v union i))
            case (Some(v), None)    => Some(d -> v)
            case (None, Some(i))    => Some(d -> i)
            case (None, None)       => None
          }
          r.asInstanceOf[Option[(Dimension, SortedSetIterator[_])]]

        case r: RawDimension[_] =>
          getIncValues(r).map(vs => r -> vs)
      }.toMap
    }

    def build(valuesToIds: (DictionaryDimension, SortedSetIterator[String]) => SortedSetIterator[Long]): Filters = {
      new Filters(includeFilter(valuesToIds), excludeFilter(valuesToIds), incTime, excTime)
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
