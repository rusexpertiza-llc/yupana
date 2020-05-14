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

package org.yupana.core.operations

import org.yupana.api.types.{ AggregationImpl, Aggregations }

trait AggregationsImpl extends Aggregations {

  override def sum[T](implicit n: Numeric[T]): AggregationImpl[T, T, T] =
    new AggregationImpl(identity, n.plus, identity, defaultIfNone[T](_, implicitly[Numeric[T]].zero))

  override def min[T](implicit o: Ordering[T]): AggregationImpl[T, T, T] =
    new AggregationImpl(identity, o.min, identity, identity)

  override def max[T](implicit o: Ordering[T]): AggregationImpl[T, T, T] =
    new AggregationImpl(identity, o.max, identity, identity)

  override def count[T]: AggregationImpl[T, Long, Long] =
    new AggregationImpl(_ => 1L, _ + _, identity, defaultIfNone[Long](_, 0L))

  override def distinctCount[T]: AggregationImpl[T, Set[T], Int] =
    new AggregationImpl(x => Set(x), _ union _, _.size, defaultIfNone[Int](_, 0))

  override def distinctRandom[T]: AggregationImpl[T, Set[T], T] =
    new AggregationImpl(x => Set(x), _ union _, random, identity)

  def random[T](s: Set[T]): T = {
    val n = util.Random.nextInt(s.size)
    s.iterator.drop(n).next
  }

  def defaultIfNone[T](opt: Option[T], default: T): Option[T] = opt.orElse(Some(default))
}
