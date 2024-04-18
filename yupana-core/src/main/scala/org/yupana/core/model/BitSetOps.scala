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

import jdk.internal.vm.annotation.ForceInline

object BitSetOps {

  @ForceInline
  def check(bitset: Array[Long], bitNum: Int): Boolean = {
    val index = bitNum / 64
    val bit = bitNum % 64
    val l = bitset(index)
    (l & (1L << bit)) != 0
  }

  @ForceInline
  def set(bitset: Array[Long], bitNum: Int): Unit = {
    val index = bitNum / 64
    val bit = bitNum % 64
    bitset(index) |= (1L << bit)
  }

  @ForceInline
  def clear(bitset: Array[Long], bitNum: Int): Unit = {
    val index = bitNum / 64
    val bit = bitNum % 64
    val mask = java.lang.Long.rotateLeft(0xFFFFFFFE, bit)
    bitset(index) &= mask
  }
}
