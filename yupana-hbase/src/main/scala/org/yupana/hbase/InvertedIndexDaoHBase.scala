package org.yupana.hbase

import org.apache.hadoop.hbase.client.{Get, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.{CellUtil, HColumnDescriptor, HTableDescriptor}
import org.yupana.core.dao.InvertedIndexDao

import scala.collection.JavaConverters._

object InvertedIndexDaoHBase {
  val FAMILY: Array[Byte] = Bytes.toBytes("f")
  val VALUE: Array[Byte] = Array.emptyByteArray

  def stringSerializer(str: String): Array[Byte] = Bytes.toBytes(str)
  def stringDeserializer(bytes: Array[Byte]): String = Bytes.toString(bytes)
  def intSerializer(v: Int): Array[Byte] = Bytes.toBytes(v)
  def intDeserializer(bytes: Array[Byte]): Int = Bytes.toInt(bytes)
  def longSerializer(v: Long): Array[Byte] = Bytes.toBytes(v)
  def longDeserializer(bytes: Array[Byte]): Long = Bytes.toLong(bytes)

  def checkTableExistsElseCreate(hBaseConnection: ExternalLinkHBaseConnection, tableName: String) {
    val desc = new HTableDescriptor(hBaseConnection.getTableName(tableName))
      .addFamily(new HColumnDescriptor(InvertedIndexDaoHBase.FAMILY))
    hBaseConnection.checkTablesExistsElseCreate(desc)
  }

  def createPutOperation[K, V](key: K, values: Set[V], keySerializer: K => Array[Byte],
               valueSerializer: V => Array[Byte]): Option[Put] = {
    if (values.isEmpty) {
      None
    } else {
      val wordBytes = keySerializer(key)
      val put = values.foldLeft(new Put(wordBytes))((put, value) =>
        put.addColumn(FAMILY, valueSerializer(value), VALUE)
      )
      Some(put)
    }
  }
}

class InvertedIndexDaoHBase[K, V](
  connection: ExternalLinkHBaseConnection,
  tableName: String,
  keySerializer: K => Array[Byte],
  valueSerializer: V => Array[Byte],
  valueDeserializer: Array[Byte] => V
) extends InvertedIndexDao[K, V] {

  import InvertedIndexDaoHBase._

  checkTableExistsElseCreate(connection, tableName)

  override def put(key: K, values: Set[V]): Unit = {
    if (values.nonEmpty) {
      val table = connection.getTable(tableName)
      val put = createPutOperation(key, values, keySerializer, valueSerializer)
      put.foreach(table.put)
    }
  }

  override def batchPut(batch: Map[K, Set[V]]): Unit = {
    val table = connection.getTable(tableName)
    val puts = batch.flatMap { case (key, values) =>
      createPutOperation(key, values, keySerializer, valueSerializer)
    }
    table.put(puts.toSeq.asJava)
  }

  private def toIterator(result: Result): Iterator[V] = {
    new Iterator[V] {
      private val cellScanner = result.cellScanner()
      private var nextResultOpt: Option[V] = None

      override def hasNext: Boolean = {
        if (nextResultOpt.isEmpty) {
          if (cellScanner.advance()) {
            nextResultOpt = Some(valueDeserializer(CellUtil.cloneQualifier(cellScanner.current())))
          }
        }
        nextResultOpt.nonEmpty
      }

      override def next(): V = {
        val result = nextResultOpt.get
        nextResultOpt = None
        result
      }
    }
  }

  override def values(key: K): Iterator[V] = {
    val get = new Get(keySerializer(key)).addFamily(FAMILY)
    val table = connection.getTable(tableName)
    toIterator(table.get(get))
  }

  override def allValues(keys: Set[K]): Seq[V] = {
    val gets = keys.map(key => new Get(keySerializer(key)).addFamily(FAMILY)).toList.asJava
    val table = connection.getTable(tableName)
    table.get(gets).flatMap(toIterator)
  }
}
