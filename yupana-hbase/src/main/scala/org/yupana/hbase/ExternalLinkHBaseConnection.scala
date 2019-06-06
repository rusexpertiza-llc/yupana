package org.yupana.hbase

import java.io.IOException

import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Table}
import org.apache.hadoop.hbase.{HTableDescriptor, TableExistsException, TableName}

class ExternalLinkHBaseConnection(val config: Configuration, namespace: String) extends StrictLogging {
  protected lazy val connection: Connection = createConnectionAndNamespace

  private def createConnectionAndNamespace = {
    val connection = ConnectionFactory.createConnection(config)
    HBaseUtils.checkNamespaceExistsElseCreate(connection, namespace)
    connection
  }

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

  def checkTablesExistsElseCreate(tableDescriptor: HTableDescriptor): Unit = {
    try {
      if (!connection.getAdmin.tableExists(tableDescriptor.getTableName)) {
        connection.getAdmin.createTable(tableDescriptor)
      }
    } catch {
      case e: TableExistsException =>
    }
  }
}
