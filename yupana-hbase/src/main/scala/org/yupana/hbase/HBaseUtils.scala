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
import org.apache.hadoop.hbase.client.metrics.ScanMetrics
import org.apache.hadoop.hbase.client.{ Table => _, _ }
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.util.Bytes
import org.joda.time.{ DateTimeZone, LocalDateTime }
import org.yupana.api.query.DataPoint
import org.yupana.api.schema._
import org.yupana.api.utils.ResourceUtils.using
import org.yupana.core.TsdbConfig
import org.yupana.core.dao.DictionaryProvider
import org.yupana.core.utils.{ CloseableIterator, CollectionUtils, QueryUtils }

import java.nio.ByteBuffer
import scala.collection.AbstractIterator
import scala.collection.JavaConverters._
import scala.collection.immutable.NumericRange

object HBaseUtils extends StrictLogging {

  type TimeShiftedValue = (Long, Array[Byte])
  type TimeShiftedValues = Array[TimeShiftedValue]
  type ValuesByGroup = Map[Int, TimeShiftedValues]

  val tableNamePrefix: String = "ts_"
  val rollupStatusFamily: Array[Byte] = "v".getBytes
  val tsdbSchemaFamily: Array[Byte] = "m".getBytes
  val rollupStatusField: Array[Byte] = "st".getBytes
  val tsdbSchemaField: Array[Byte] = "meta".getBytes
  val rollupSpecialKey: Array[Byte] = "\u0000".getBytes
  val tsdbSchemaKey: Array[Byte] = "\u0000".getBytes
  private val NULL_VALUE: Long = 0L
  val TAGS_POSITION_IN_ROW_KEY: Int = Bytes.SIZEOF_LONG
  val tsdbSchemaTableName: String = tableNamePrefix + "table"

  def baseTime(time: Long, table: Table): Long = {
    time - time % table.rowTimeSpan
  }

  def restTime(time: Long, table: Table): Long = {
    time % table.rowTimeSpan
  }

  def baseTimeList(fromTime: Long, toTime: Long, table: Table): NumericRange[Long] = {
    val startBaseTime = fromTime - (fromTime % table.rowTimeSpan)
    val stopBaseTime = toTime - (toTime % table.rowTimeSpan)
    startBaseTime to stopBaseTime by table.rowTimeSpan
  }

  def createPuts(
      dataPoints: Seq[DataPoint],
      dictionaryProvider: DictionaryProvider
  ): Seq[(Table, Seq[Put])] = {
    dataPoints
      .groupBy(_.table)
      .map {
        case (table, points) =>
          val keySize = tableKeySize(table)
          val grouped = points.groupBy(rowKey(_, table, keySize, dictionaryProvider))
          table -> grouped.map {
            case (key, dps) => createPutOperation(table, key, dps)
          }.toSeq
      }
      .toSeq
  }

  def createPutOperation(table: Table, key: Array[Byte], dataPoints: Seq[DataPoint]): Put = {
    val put = new Put(key)
    valuesByGroup(table, dataPoints).foreach {
      case (group, values) =>
        values.foreach {
          case (time, bytes) =>
            val timeBytes = Bytes.toBytes(time)
            put.addColumn(family(group), timeBytes, bytes)
        }
    }
    put
  }

  def createScan(
      queryContext: InternalQueryContext,
      multiRowRangeFilter: Option[MultiRowRangeFilter],
      hbaseFuzzyRowFilter: Seq[FuzzyRowFilter],
      fromTime: Long,
      toTime: Long,
      startRowKey: Option[Array[Byte]] = None,
      endRowKey: Option[Array[Byte]] = None
  ): Option[Scan] = {

    logger.trace(s"Create range scan for ${multiRowRangeFilter.map(_.getRowRanges.size())} ranges")

    val rangeStartKey = multiRowRangeFilter.map(_.getRowRanges.asScala.head.getStartRow)
    val rangeStopKey = multiRowRangeFilter.map(_.getRowRanges.asScala.toList.last.getStopRow)

    val fromTimeKey = Bytes.toBytes(baseTime(fromTime, queryContext.table))
    val toTimeKey = Bytes.toBytes(baseTime(toTime, queryContext.table) + 1)

    val startKey = List(rangeStartKey, Some(fromTimeKey), startRowKey).flatten
      .max(Ordering.comparatorToOrdering(Bytes.BYTES_COMPARATOR))

    val stopKey = List(rangeStopKey, Some(toTimeKey), endRowKey.filter(_.nonEmpty)).flatten
      .min(Ordering.comparatorToOrdering(Bytes.BYTES_COMPARATOR))

    val filter = multiRowRangeFilter match {
      case Some(rangeFilter) =>
        if (hbaseFuzzyRowFilter.nonEmpty) {
          val orFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE, hbaseFuzzyRowFilter: _*)
          Some(new FilterList(FilterList.Operator.MUST_PASS_ALL, rangeFilter, orFilter))
        } else {
          Some(rangeFilter)
        }
      case None =>
        if (hbaseFuzzyRowFilter.nonEmpty) {
          Some(new FilterList(FilterList.Operator.MUST_PASS_ONE, hbaseFuzzyRowFilter: _*))
        } else {
          None
        }
    }

    if (Bytes.BYTES_COMPARATOR.compare(startKey, stopKey) < 0) {
      val scan = new Scan().withStartRow(startKey).withStopRow(stopKey)
      filter.foreach(scan.setFilter)

      familiesQueried(queryContext).foreach(f => scan.addFamily(HBaseUtils.family(f)))
      Some(scan)
    } else None
  }

  def executeScan(
      connection: Connection,
      namespace: String,
      scan: Scan,
      context: InternalQueryContext,
      batchSize: Int
  ): Iterator[Result] = {

    val htable = connection.getTable(tableName(namespace, context.table))
    scan.setScanMetricsEnabled(context.metricsCollector.isEnabled)
    val scanner = htable.getScanner(scan)

    def close(): Unit = {
      scanner.close()
      htable.close()
    }

    val scannerIterator = scanner.iterator()
    val batchIterator = scannerIterator.asScala.grouped(batchSize)

    val resultIterator = new AbstractIterator[List[Result]] {
      override def hasNext: Boolean = {
        context.metricsCollector.scan.measure(1) {
          val hasNext = batchIterator.hasNext
          if (!hasNext && scan.isScanMetricsEnabled) {
            logger.info(
              s"query_uuid: ${context.metricsCollector.queryId}, scans: ${scanMetricsToString(scanner.getScanMetrics)}"
            )
          }
          hasNext
        }
      }

      override def next(): List[Result] = {
        batchIterator.next()
      }
    }

    CloseableIterator(resultIterator.flatten, close())
  }

  def multiRowRangeFilter(
      table: Table,
      from: Long,
      to: Long,
      dimIds: Map[Dimension, Seq[_]]
  ): Option[MultiRowRangeFilter] = {

    val baseTimeLs = baseTimeList(from, to, table)

    val dimIdsList = dimIds.toList.map {
      case (dim, ids) =>
        dim -> ids.toList.map(id => dim.rStorable.write(id.asInstanceOf[dim.R]))
    }

    val crossJoinedDimIds = {
      CollectionUtils.crossJoin(dimIdsList.map(_._2))
    }

    val keySize = tableKeySize(table)

    val ranges = for {
      time <- baseTimeLs
      cids <- crossJoinedDimIds if cids.nonEmpty
    } yield {
      rowRange(time, keySize, cids.toArray)
    }

    if (ranges.nonEmpty) {
      val filter = new MultiRowRangeFilter(new java.util.ArrayList(ranges.asJava))
      Some(filter)
    } else {
      None
    }
  }

  private def rowRange(baseTime: Long, keySize: Int, dimIds: Array[Array[Byte]]): RowRange = {
    val tmpBuffer = ByteBuffer.allocate(keySize)

    tmpBuffer.put(Bytes.toBytes(baseTime))
    dimIds.foreach { dimBytes =>
      tmpBuffer.put(dimBytes)
    }

    val bytes = new Array[Byte](tmpBuffer.position())
    tmpBuffer.rewind()
    tmpBuffer.get(bytes)

    val startBuffer = ByteBuffer.allocate(keySize)
    val stopBuffer = ByteBuffer.allocate(keySize)
    startBuffer.put(bytes)
    stopBuffer.put(Bytes.unsignedCopyAndIncrement(bytes))

    new RowRange(startBuffer.array(), true, stopBuffer.array(), false)
  }

  private def familiesQueried(queryContext: InternalQueryContext): Seq[Int] = {
    val groups = queryContext.exprsIndexSeq.flatMap { case (e, _) => QueryUtils.requiredMetrics(e).map(_.group) }
    if (groups.nonEmpty) {
      groups
    } else {
      Seq(Metric.defaultGroup)
    }
  }

  def tableNameString(namespace: String, table: Table): String = {
    tableName(namespace, table).getNameAsString
  }

  def tableName(namespace: String, table: Table): TableName = {
    TableName.valueOf(namespace, tableNamePrefix + table.name)
  }

  def parseRowKey(bytes: Array[Byte], table: Table): TSDRowKey = {
    val baseTime = Bytes.toLong(bytes)

    val dimReprs = Array.ofDim[Option[Any]](table.dimensionSeq.size)

    var i = 0
    val bb = ByteBuffer.wrap(bytes, TAGS_POSITION_IN_ROW_KEY, bytes.length - TAGS_POSITION_IN_ROW_KEY)
    table.dimensionSeq.foreach { dim =>
      val value = dim.rStorable.read(bb)
      dimReprs(i) = Some(value)
      i += 1
    }
    TSDRowKey(baseTime, dimReprs)
  }

  private def checkSchemaDefinition(connection: Connection, namespace: String, schema: Schema): SchemaCheckResult = {

    val metaTableName = TableName.valueOf(namespace, tsdbSchemaTableName)

    using(connection.getAdmin) { admin =>
      if (admin.tableExists(metaTableName)) {
        ProtobufSchemaChecker.check(schema, readTsdbSchema(connection, namespace))
      } else {

        val tsdbSchemaBytes = ProtobufSchemaChecker.toBytes(schema)
        ProtobufSchemaChecker.check(schema, tsdbSchemaBytes)

        writeTsdbSchema(connection, namespace, tsdbSchemaBytes)
        Success
      }
    }
  }

  def readTsdbSchema(connection: Connection, namespace: String): Array[Byte] = {
    using(connection.getTable(TableName.valueOf(namespace, tsdbSchemaTableName))) { table =>
      val get = new Get(tsdbSchemaKey).addColumn(tsdbSchemaFamily, tsdbSchemaField)
      table.get(get).getValue(tsdbSchemaFamily, tsdbSchemaField)
    }
  }

  def writeTsdbSchema(connection: Connection, namespace: String, schemaBytes: Array[Byte]): Unit = {
    logger.info(s"Writing TSDB Schema definition to namespace $namespace")

    val metaTableName = TableName.valueOf(namespace, tsdbSchemaTableName)
    using(connection.getAdmin) { admin =>
      if (!admin.tableExists(metaTableName)) {
        val tableDesc = TableDescriptorBuilder
          .newBuilder(metaTableName)
          .setColumnFamily(
            ColumnFamilyDescriptorBuilder
              .newBuilder(tsdbSchemaFamily)
              .setDataBlockEncoding(DataBlockEncoding.PREFIX)
              .build()
          )
          .build()
        admin.createTable(tableDesc)
      }
      using(connection.getTable(metaTableName)) { table =>
        val put = new Put(tsdbSchemaKey).addColumn(tsdbSchemaFamily, tsdbSchemaField, schemaBytes)
        table.put(put)
      }
    }
  }

  def initStorage(connection: Connection, namespace: String, schema: Schema, config: TsdbConfig): Unit = {
    checkNamespaceExistsElseCreate(connection, namespace)

    val dictDao = new DictionaryDaoHBase(connection, namespace)

    schema.tables.values.foreach { t =>
      checkTableExistsElseCreate(connection, namespace, t, config.maxRegions, config.compression)
      checkRollupStatusFamilyExistsElseCreate(connection, namespace, t)
      t.dimensionSeq.foreach(dictDao.checkTablesExistsElseCreate)
    }
    checkSchemaDefinition(connection, namespace, schema) match {
      case Success      => logger.info("TSDB table definition checked successfully")
      case Warning(msg) => logger.warn("TSDB table definition check warnings: " + msg)
      case Error(msg)   => throw new RuntimeException("TSDB table definition check failed: " + msg)
    }
  }

  def checkNamespaceExistsElseCreate(connection: Connection, namespace: String): Unit = {
    using(connection.getAdmin) { admin =>
      if (!admin.listNamespaceDescriptors.exists(_.getName == namespace)) {
        val namespaceDescriptor = NamespaceDescriptor.create(namespace).build()
        admin.createNamespace(namespaceDescriptor)
      }
    }
  }

  def checkTableExistsElseCreate(
      connection: Connection,
      namespace: String,
      table: Table,
      maxRegions: Int,
      compressionAlgorithm: String
  ): Unit = {
    val hbaseTable = tableName(namespace, table)
    val algorithm = Compression.getCompressionAlgorithmByName(compressionAlgorithm)
    using(connection.getAdmin) { admin =>
      if (!admin.tableExists(hbaseTable)) {
        val fieldGroups = table.metrics.map(_.group).toSet
        val families = fieldGroups map (group =>
          ColumnFamilyDescriptorBuilder
            .newBuilder(family(group))
            .setDataBlockEncoding(DataBlockEncoding.PREFIX)
            .setCompactionCompressionType(algorithm)
            .build()
        )
        val desc = TableDescriptorBuilder
          .newBuilder(hbaseTable)
          .setColumnFamilies(families.asJavaCollection)
          .build()
        val endTime = new LocalDateTime()
          .plusYears(1)
          .withMonthOfYear(1)
          .withDayOfMonth(1)
          .withTime(0, 0, 0, 0)
          .toDateTime(DateTimeZone.UTC)
          .getMillis
        val r = ((endTime - table.epochTime) / table.rowTimeSpan).toInt * 10
        val regions = math.min(r, maxRegions)
        admin.createTable(
          desc,
          Bytes.toBytes(baseTime(table.epochTime, table)),
          Bytes.toBytes(baseTime(endTime, table)),
          regions
        )
      }
    }
  }

  def checkRollupStatusFamilyExistsElseCreate(connection: Connection, namespace: String, table: Table): Unit = {
    val name = tableName(namespace, table)
    using(connection.getTable(name)) { hbaseTable =>
      val tableDesc = hbaseTable.getDescriptor
      if (!tableDesc.hasColumnFamily(rollupStatusFamily)) {
        using(connection.getAdmin) {
          _.addColumnFamily(
            name,
            ColumnFamilyDescriptorBuilder
              .newBuilder(rollupStatusFamily)
              .setDataBlockEncoding(DataBlockEncoding.PREFIX)
              .build()
          )
        }
      }
    }
  }

  def createRollupStatusPut(time: Long, status: String): Put = {
    new Put(Bytes.toBytes(time)).addColumn(rollupStatusFamily, rollupStatusField, status.getBytes)
  }

  private[hbase] def tableKeySize(table: Table): Int = {
    Bytes.SIZEOF_LONG + table.dimensionSeq.map(_.rStorable.size).sum
  }

  private[hbase] def rowKey(
      dataPoint: DataPoint,
      table: Table,
      keySize: Int,
      dictionaryProvider: DictionaryProvider
  ): Array[Byte] = {
    val bt = HBaseUtils.baseTime(dataPoint.time, table)
    val baseTimeBytes = Bytes.toBytes(bt)

    val buffer = ByteBuffer
      .allocate(keySize)
      .put(baseTimeBytes)

    table.dimensionSeq.foreach { dim =>
      val bytes = dim match {
        case dd: DictionaryDimension =>
          val id = dataPoint.dimensions
            .get(dim)
            .asInstanceOf[Option[String]]
            .filter(_.trim.nonEmpty)
            .map(v => dictionaryProvider.dictionary(dd).id(v))
            .getOrElse(NULL_VALUE)
          Bytes.toBytes(id)

        case rd: RawDimension[_] =>
          val v = dataPoint.dimensions(dim).asInstanceOf[rd.T]
          rd.rStorable.write(v)

        case hd: HashDimension[_, _] =>
          val v = dataPoint.dimensions(dim).asInstanceOf[hd.T]
          val hash = hd.hashFunction(v).asInstanceOf[hd.R]
          hd.rStorable.write(hash)
      }

      buffer.put(bytes)
    }

    buffer.array()
  }

  private def scanMetricsToString(metrics: ScanMetrics): String = {
    import scala.collection.JavaConverters._
    metrics.getMetricsMap.asScala.map { case (k, v) => s""""$k":"$v"""" }.mkString("{", ",", "}")
  }

  def family(group: Int): Array[Byte] = s"d$group".getBytes

  def valuesByGroup(table: Table, dataPoints: Seq[DataPoint]): ValuesByGroup = {
    dataPoints.map(partitionValuesByGroup(table)).reduce(mergeMaps).mapValues(_.toArray)
  }

  private def partitionValuesByGroup(table: Table)(dp: DataPoint): Map[Int, Seq[TimeShiftedValue]] = {
    val timeShift = HBaseUtils.restTime(dp.time, table)
    dp.metrics
      .groupBy(_.metric.group)
      .mapValues(metricValues => Seq((timeShift, fieldsToBytes(table, dp.dimensions, metricValues))))
  }

  private def mergeMaps(
      m1: Map[Int, Seq[TimeShiftedValue]],
      m2: Map[Int, Seq[TimeShiftedValue]]
  ): Map[Int, Seq[TimeShiftedValue]] = {
    (m1.keySet ++ m2.keySet).map(k => (k, m1.getOrElse(k, Seq.empty) ++ m2.getOrElse(k, Seq.empty))).toMap
  }

  private def fieldsToBytes(
      table: Table,
      dimensions: Map[Dimension, Any],
      metricValues: Seq[MetricValue]
  ): Array[Byte] = {
    val metricFieldBytes = metricValues.map { f =>
      val bytes = f.metric.dataType.storable.write(f.value)
      (f.metric.tag, bytes)
    }
    val dimensionFieldBytes = dimensions.collect {
      case (d: DictionaryDimension, value) if table.dimensionTagExists(d) =>
        val tag = table.dimensionTag(d)
        val bytes = d.dataType.storable.write(value.asInstanceOf[d.T])
        (tag, bytes)
      case (d: HashDimension[_, _], value) if table.dimensionTagExists(d) =>
        val tag = table.dimensionTag(d)
        val bytes = d.tStorable.write(value.asInstanceOf[d.T])
        (tag, bytes)
    }

    val fieldBytes = metricFieldBytes ++ dimensionFieldBytes

    val size = fieldBytes.map(_._2.length).sum + fieldBytes.size
    val bb = ByteBuffer.allocate(size)
    fieldBytes.foreach {
      case (tag, bytes) =>
        bb.put(tag)
        bb.put(bytes)
    }

    bb.array()
  }
}
