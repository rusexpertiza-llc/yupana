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

import org.apache.hadoop.hbase.client.{Get, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor}

import scala.collection.JavaConverters._

class BTreeIndexDaoHBase[K, V](
  connection: ExternalLinkHBaseConnection,
  tableName: String,
  keySerializer: K => Array[Byte],
  valueSerializer: V => Array[Byte],
  valueDeserializer: Array[Byte] => V
) {

  val FAMILY: Array[Byte] = Bytes.toBytes("f")
  val QUALIFIER: Array[Byte] = Bytes.toBytes("d")

  checkTableExistsElseCreate()

  def put(key: K, value: V): Unit = {
    val table = connection.getTable(tableName)
    val put = createPutOperation(key, value)
    table.put(put)
  }

  private def createPutOperation(key: K, value: V) = {
    new Put(keySerializer(key)).addColumn(FAMILY, QUALIFIER, valueSerializer(value))
  }

  def batchPut(batch: Seq[(K,V)]) = {
    val puts = batch.map { case (key, value) => createPutOperation(key, value) }
    val table = connection.getTable(tableName)
    table.put(puts.asJava)
  }

  def get(key: K): Option[V] = {
    val table = connection.getTable(tableName)
    val get = new Get(keySerializer(key)).addColumn(FAMILY, QUALIFIER)
    val result = table.get(get)
    Option(result.getValue(FAMILY, QUALIFIER)).map(valueDeserializer)
  }

  def get(keys: Seq[K]): Seq[V] = {
    val gets = keys.map(key => new Get(keySerializer(key)).addColumn(FAMILY, QUALIFIER))
    val table = connection.getTable(tableName)
    val result = table.get(gets.asJava)
    result.flatMap(r => Option(r.getValue(FAMILY, QUALIFIER)).map(valueDeserializer))
  }

  private def checkTableExistsElseCreate() {
    val descriptor = new HTableDescriptor(connection.getTableName(tableName))
      .addFamily(new HColumnDescriptor(FAMILY))
    connection.checkTablesExistsElseCreate(descriptor)
  }
}
