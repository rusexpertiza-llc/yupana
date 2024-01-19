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

package org.yupana.khipu.storage

case class Row(key: MemorySegment, value: MemorySegment) {
  def keyBytes: Array[Byte] = {
    StorageFormat.getBytes(key, 0, key.byteSize().toInt)
  }

  def valueBytes: Array[Byte] = {
    StorageFormat.getBytes(value, 0, value.byteSize().toInt)
  }
}

object Row {
  def apply(key: Array[Byte], value: Array[Byte]): Row = {

    val keySegment = StorageFormat.fromBytes(key)
    val valueSegment = StorageFormat.fromBytes(value)

    Row(keySegment, valueSegment)
  }
}
