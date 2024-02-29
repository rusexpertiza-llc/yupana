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

import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter
import org.apache.hadoop.hbase.util.Bytes
import org.yupana.api.schema.Dimension
import org.yupana.core.dao.DictionaryDao

import scala.jdk.CollectionConverters._
import scala.util.Using

object DictionaryDaoHBase {

  val tableNamePrefix: String = "ts_dict_"
  val dataFamily: Array[Byte] = Bytes.toBytes("d") // store value -> id
  val seqFamily: Array[Byte] = Bytes.toBytes("seq") // store last id
  val column: Array[Byte] = Bytes.toBytes("c")
  val seqIdRowKey: Array[Byte] = Bytes.toBytes(0)

  val BATCH_SIZE = 50000

  def getTableName(namespace: String, name: String): TableName =
    TableName.valueOf(namespace, tableNamePrefix + name)

  def getReversePairFromResult(result: Result): Option[(Long, String)] = {
    if (!result.isEmpty) {
      Option(result.getValue(dataFamily, column))
        .map(Bytes.toLong)
        .map((_, Bytes.toString(result.getRow)))
    } else {
      None
    }
  }

  def getReverseScan: Scan = new Scan().addFamily(DictionaryDaoHBase.dataFamily)
}

class DictionaryDaoHBase(connection: Connection, namespace: String) extends DictionaryDao with StrictLogging {
  import DictionaryDaoHBase._

  var existsTables = Set.empty[String]

  HBaseUtils.checkNamespaceExistsElseCreate(connection, namespace)

  override def getIdByValue(dimension: Dimension, value: String): Option[Long] = {
    checkTablesExistsElseCreate(dimension)
    if (value != null) {
      val trimmed = value.trim
      if (trimmed.nonEmpty) {
        Using.resource(getTable(dimension.name)) { table =>
          val get = new Get(Bytes.toBytes(trimmed)).addFamily(dataFamily)
          val result = table.get(get)
          if (!result.isEmpty) {
            Some(Bytes.toLong(result.getValue(dataFamily, column)))
          } else {
            None
          }
        }
      } else {
        None
      }
    } else {
      None
    }
  }

  override def getIdsByValues(dimension: Dimension, values: Set[String]): Map[String, Long] = {
    if (values.isEmpty) {
      Map.empty
    } else {
      val nonEmptyValues = values.filter(_ != null).map(_.trim).filter(_.nonEmpty).toSeq
      logger.trace(s"Get dictionary ids by values for ${dimension.name}. Size of values: ${nonEmptyValues.size}")
      checkTablesExistsElseCreate(dimension)
      val r = Using.resource(getTable(dimension.name)) { table =>
        nonEmptyValues
          .grouped(BATCH_SIZE)
          .flatMap { vs =>
            val ranges = vs.map { value =>
              new MultiRowRangeFilter.RowRange(Bytes.toBytes(value), true, Bytes.toBytes(value), true)
            }

            val rangeFilter = new MultiRowRangeFilter(new java.util.ArrayList(ranges.asJava))
            val start = rangeFilter.getRowRanges.asScala.head.getStartRow
            val end = Bytes.padTail(rangeFilter.getRowRanges.asScala.last.getStopRow, 1)
            val scan = new Scan()
              .withStartRow(start)
              .withStopRow(end)
              .addFamily(dataFamily)
              .setFilter(rangeFilter)

            logger.trace(s"--- Send request to HBase")
            Using.resource(table.getScanner(scan)) {
              _.iterator().asScala
                .map { result =>
                  val id = Bytes.toLong(result.getValue(dataFamily, column))
                  val value = Bytes.toString(result.getRow)
                  value -> id
                }
                .toSeq
            }
          }
          .toMap
      }

      logger.trace(s"--- Dictionary values extracted")
      r
    }
  }

  override def checkAndPut(dimension: Dimension, id: Long, value: String): Boolean = {
    checkTablesExistsElseCreate(dimension)
    val idBytes = Bytes.toBytes(id)
    val valueBytes = Bytes.toBytes(value)

    Using.resource(getTable(dimension.name)) { table =>
      val rput = new Put(valueBytes).addColumn(dataFamily, column, idBytes)
      table.checkAndMutate(CheckAndMutate.newBuilder(valueBytes).ifNotExists(dataFamily, column).build(rput)).isSuccess
    }
  }

  override def createSeqId(dimension: Dimension): Int = {
    checkTablesExistsElseCreate(dimension)
    Using.resource(getTable(dimension.name))(_.incrementColumnValue(seqIdRowKey, seqFamily, column, 1).toInt)
  }

  private def getTable(name: String) = {
    connection.getTable(getTableName(namespace, name))
  }

  def checkTablesExistsElseCreate(dimension: Dimension): Unit = {
    if (!existsTables.contains(dimension.name)) {
      HBaseUtils.checkTableExistsElseCreate(
        connection,
        getTableName(namespace, dimension.name),
        Seq(seqFamily, dataFamily)
      )
      existsTables += dimension.name
    }
  }
}
