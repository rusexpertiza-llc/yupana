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

package org.yupana.core.utils

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CollectionUtilsTest extends AnyFlatSpec with Matchers {

  "Cross join" should "join single list" in {
    val kkms = (1 to 3).map("kkmId" -> _.toString).toList
    val cj = CollectionUtils.crossJoin(List(kkms))

    cj should contain theSameElementsInOrderAs List(
      List("kkmId" -> "1"),
      List("kkmId" -> "2"),
      List("kkmId" -> "3")
    )
  }

  it should "join two lists" in {
    val kkms = (1 to 3).map("kkmId" -> _).toList
    val items = (1 to 2).map("item" -> _).toList
    val cj = CollectionUtils.crossJoin(List(kkms, items))

    cj should contain theSameElementsInOrderAs List(
      List("kkmId" -> 1, "item" -> 1),
      List("kkmId" -> 1, "item" -> 2),
      List("kkmId" -> 2, "item" -> 1),
      List("kkmId" -> 2, "item" -> 2),
      List("kkmId" -> 3, "item" -> 1),
      List("kkmId" -> 3, "item" -> 2)
    )
  }

  it should "join three lists" in {
    val as = (2 to 3).map("a" -> _).toList
    val bs = (1 to 2).map("b" -> _).toList
    val cs = (5 to 6).map("c" -> _).toList
    val cj = CollectionUtils.crossJoin(List(as, bs, cs))

    cj should contain theSameElementsInOrderAs List(
      List("a" -> 2, "b" -> 1, "c" -> 5),
      List("a" -> 2, "b" -> 1, "c" -> 6),
      List("a" -> 2, "b" -> 2, "c" -> 5),
      List("a" -> 2, "b" -> 2, "c" -> 6),
      List("a" -> 3, "b" -> 1, "c" -> 5),
      List("a" -> 3, "b" -> 1, "c" -> 6),
      List("a" -> 3, "b" -> 2, "c" -> 5),
      List("a" -> 3, "b" -> 2, "c" -> 6)
    )
  }

  "Align by key" should "align" in {
    val keys = List("kkm", "item", "op_type")
    val values = List("item" -> 6, "kkm" -> 1, "op_type" -> 2)
    CollectionUtils.alignByKey(keys, values, (x: (String, Int)) => x._1) shouldEqual List(
      "kkm" -> 1,
      "item" -> 6,
      "op_type" -> 2
    )
  }

  it should "work if some key is missing" in {
    val keys = List("kkm", "item", "op_type")
    val values = List("op_type" -> 77, "item" -> 6)
    CollectionUtils.alignByKey(keys, values, (x: (String, Int)) => x._1) shouldEqual List("item" -> 6, "op_type" -> 77)
  }

  it should "throw exception on unknown tag" in {
    val keys = List("kkm", "item", "op_type")
    val values = List("customer" -> 4, "item" -> 6)
    an[IllegalArgumentException] should be thrownBy CollectionUtils.alignByKey(keys, values, (x: (String, Int)) => x._1)
  }

  it should "throw exception if reduce stage is too big" in {
    val it = (0 to 10).map(i => i -> i).iterator
    an[IllegalStateException] should be thrownBy CollectionUtils.reduceByKey(it, 2)(_ + _)
  }
}
