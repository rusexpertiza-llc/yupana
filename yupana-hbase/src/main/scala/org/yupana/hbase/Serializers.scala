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

import org.apache.hadoop.hbase.util.Bytes

object Serializers {

  def stringSerializer(str: String): Array[Byte] = Bytes.toBytes(str)
  def stringDeserializer(bytes: Array[Byte]): String = Bytes.toString(bytes)
  def intSerializer(v: Int): Array[Byte] = Bytes.toBytes(v)
  def intDeserializer(bytes: Array[Byte]): Int = Bytes.toInt(bytes)
  def longSerializer(v: Long): Array[Byte] = Bytes.toBytes(v)
  def longDeserializer(bytes: Array[Byte]): Long = Bytes.toLong(bytes)

}
