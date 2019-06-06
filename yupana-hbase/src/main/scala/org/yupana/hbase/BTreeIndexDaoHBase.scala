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
    val put = new Put(keySerializer(key)).addColumn(FAMILY, QUALIFIER, valueSerializer(value))
    table.put(put)
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
