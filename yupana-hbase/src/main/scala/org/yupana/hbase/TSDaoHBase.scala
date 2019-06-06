package org.yupana.hbase

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.metrics.ScanMetrics
import org.apache.hadoop.hbase.client.{Connection, Get, Put, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.yupana.api.query.DataPoint
import org.yupana.api.schema.Table
import org.yupana.core.MapReducible
import org.yupana.core.dao.{DictionaryProvider, TSDao}
import org.yupana.core.utils.metric.MetricQueryCollector
import org.yupana.hbase.HBaseUtils._

import scala.collection.AbstractIterator
import scala.collection.JavaConverters._

class TSDaoHBase(connection: Connection,
                 namespace: String,
                 override val dictionaryProvider: DictionaryProvider,
                 putsBatchSize: Int = 1000
                ) extends TSDaoHBaseBase[Iterator] with TSDao[Iterator, Long] {

  override val mr: MapReducible[Iterator] = MapReducible.iteratorMR

  override def executeScans(table: Table, scans: Seq[Scan], metricCollector: MetricQueryCollector): Iterator[TSDOutputRow[Long]] = {
    import HBaseUtils._

    if (scans.nonEmpty) {
      val htable = connection.getTable(tableName(namespace, table))

      scans.iterator.flatMap { scan =>
        scan.setScanMetricsEnabled(metricCollector.isEnabled)
        val scanner = htable.getScanner(scan)
        val scannerIterator = scanner.iterator()
        val it = new AbstractIterator[TSDOutputRow[Long]] {
          override def hasNext: Boolean = metricCollector.getResult.measure {
            val hasNext = scannerIterator.hasNext
            if (!hasNext) {
              logger.info(s"query_uuid: ${metricCollector.uuid}, scans: ${scanMetricsToString(scan.getScanMetrics)}")
            }
            hasNext
          }

          override def next(): TSDOutputRow[Long] = metricCollector.parseResult.measure {
            getTsdRowFromResult(table, scannerIterator.next())
          }
        }
        it
      }
    } else {
      Iterator.empty
    }
  }

  override def put(dataPoints: Seq[DataPoint]): Unit = {
    logger.trace(s"Put ${dataPoints.size} dataPoints to tsdb")
    logger.trace(s" -- DETAIL DATAPOINTS: \r\n ${dataPoints.mkString("\r\n")}")

    createTsdRows(dataPoints, dictionaryProvider).foreach { case (table, rows) =>
      val hbaseTable = connection.getTable(tableName(namespace, table))
      rows
        .map(createPutOperation)
        .sliding(putsBatchSize, putsBatchSize)
        .foreach(putsBatch => hbaseTable.put(putsBatch.asJava))
      logger.trace(s" -- DETAIL ROWS: \r\n ${rows.mkString("\r\n")}")
    }
  }

  override def getRollupStatuses(fromTime: Long, toTime: Long, table: Table): Seq[(Long, String)] = {
    checkRollupStatusFamilyExistsElseCreate(connection, namespace, table)
    val hbaseTable = connection.getTable(tableName(namespace, table))
    val scan = new Scan()
      .addColumn(rollupStatusFamily, rollupStatusField)
      .setStartRow(Bytes.toBytes(fromTime))
      .setStopRow(Bytes.toBytes(toTime))
    val scanner = hbaseTable.getScanner(scan)
    val statuses = scanner.asScala.toIterator.flatMap { result =>
      val time = Bytes.toLong(result.getRow)
      val value = Option(result.getValue(rollupStatusFamily, rollupStatusField)).map(Bytes.toString)
      value.map(v => time -> v)
    }.toSeq
    statuses
  }

  override def putRollupStatuses(statuses: Seq[(Long, String)], table: Table): Unit = {
    checkRollupStatusFamilyExistsElseCreate(connection, namespace, table)
    val hbaseTable = connection.getTable(tableName(namespace, table))
    val puts = statuses.map(status =>
      new Put(Bytes.toBytes(status._1))
        .addColumn(rollupStatusFamily, rollupStatusField, Bytes.toBytes(status._2))
    )
    hbaseTable.put(puts.asJava)
  }

  override def checkAndPutRollupStatus(time: Long, oldStatus: Option[String], newStatus: String, table: Table): Boolean = {
    checkRollupStatusFamilyExistsElseCreate(connection, namespace, table)
    val hbaseTable = connection.getTable(tableName(namespace, table))
    hbaseTable.checkAndPut(
      Bytes.toBytes(time),
      rollupStatusFamily,
      rollupStatusField,
      oldStatus.map(_.getBytes).orNull,
      new Put(Bytes.toBytes(time)).addColumn(rollupStatusFamily, rollupStatusField, Bytes.toBytes(newStatus))
    )
  }

  override def getRollupSpecialField(fieldName: String, table: Table): Option[Long] = {
    checkRollupStatusFamilyExistsElseCreate(connection, namespace, table)
    val hbaseTable = connection.getTable(tableName(namespace, table))
    val get = new Get(rollupSpecialKey).addColumn(rollupStatusFamily, fieldName.getBytes)
    val res = hbaseTable.get(get)
    val cell = Option(res.getColumnLatestCell(rollupStatusFamily, fieldName.getBytes))
    cell.map(c => Bytes.toLong(CellUtil.cloneValue(c)))
  }

  override def putRollupSpecialField(fieldName: String, value: Long, table: Table): Unit = {
    checkRollupStatusFamilyExistsElseCreate(connection, namespace, table)
    val hbaseTable = connection.getTable(tableName(namespace, table))
    val put: Put = new Put(rollupSpecialKey).addColumn(rollupStatusFamily, fieldName.getBytes, Bytes.toBytes(value))
    hbaseTable.put(put)
  }

  private def scanMetricsToString(metrics: ScanMetrics): String = {
    import scala.collection.JavaConverters._
    metrics.getMetricsMap.asScala.map { case (k, v) => s""""$k":"$v"""" }.mkString("{", ",", "}")
  }
}
