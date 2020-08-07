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

import java.io.IOException

import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ Connection, ConnectionFactory, Table, TableDescriptor }
import org.apache.hadoop.hbase.{ HBaseConfiguration, TableExistsException, TableName }

class ExternalLinkHBaseConnection(val config: Configuration, namespace: String) extends StrictLogging {
  protected lazy val connection: Connection = createConnectionAndNamespace

  private def createConnectionAndNamespace: Connection = {
    val connection = ConnectionFactory.createConnection(config)
    HBaseUtils.checkNamespaceExistsElseCreate(connection, namespace)
    connection
  }

  def close(): Unit = connection.close()

  def getTableName(tableNameString: String): TableName =
    TableName.valueOf(namespace, tableNameString)

  def getTable(tableNameString: String): Table =
    connection.getTable(getTableName(tableNameString))

  def clearTable(tableNameString: String): Unit = {
    val tableName = getTableName(tableNameString)
    try {
      if (connection.getAdmin.isTableEnabled(tableName)) {
        connection.getAdmin.disableTable(tableName)
      }
      connection.getAdmin.truncateTable(tableName, false)
    } catch {
      case e: IOException =>
    }
  }

  def checkTablesExistsElseCreate(tableDescriptor: TableDescriptor): Unit = {
    try {
      if (!connection.getAdmin.tableExists(tableDescriptor.getTableName)) {
        connection.getAdmin.createTable(tableDescriptor)
      }
    } catch {
      case e: TableExistsException =>
    }
  }
}

object ExternalLinkHBaseConnection {
  def apply(hbaseUrl: String, hBaseNamespace: String): ExternalLinkHBaseConnection = {
    val hBaseConfiguration = HBaseConfiguration.create()
    hBaseConfiguration.set("hbase.zookeeper.quorum", hbaseUrl)
    hBaseConfiguration.set("zookeeper.session.timeout", "180000")
    val hBaseConnection = new ExternalLinkHBaseConnection(hBaseConfiguration, hBaseNamespace)
    hBaseConnection
  }
}
