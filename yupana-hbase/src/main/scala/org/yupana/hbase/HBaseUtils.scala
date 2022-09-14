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
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.util.Bytes
import org.yupana.api.query.DataPoint
import org.yupana.api.schema._
import org.yupana.api.utils.ResourceUtils.using
import org.yupana.core.TsdbConfig
import org.yupana.core.dao.DictionaryProvider
import org.yupana.core.model.UpdateInterval
import org.yupana.core.utils.metric.MetricQueryCollector
import org.yupana.core.utils.{ CloseableIterator, CollectionUtils, QueryUtils }

import java.nio.ByteBuffer
import java.time.temporal.TemporalAdjusters
import java.time.{ Instant, LocalDate, OffsetDateTime, ZoneOffset }
import scala.collection.AbstractIterator
import scala.jdk.CollectionConverters._
import scala.collection.immutable.NumericRange

object HBaseUtils extends StrictLogging {

  type TimeShiftedValue = (Long, Array[Byte])
  type TimeShiftedValues = Array[TimeShiftedValue]
  type ValuesByGroup = Map[Int, TimeShiftedValues]

  val tableNamePrefix: String = "ts_"
  val tsdbSchemaFamily: Array[Byte] = "m".getBytes
  val tsdbSchemaField: Array[Byte] = "meta".getBytes
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

  def loadDimIds(dictionaryProvider: DictionaryProvider, table: Table, dataPoints: Seq[DataPoint]): Unit = {
    table.dimensionSeq.foreach {
      case dimension: DictionaryDimension =>
        val values = dataPoints.flatMap { dp =>
          dp.dimensionValue(dimension).filter(_.trim.nonEmpty)
        }
        dictionaryProvider.dictionary(dimension).findIdsByValues(values.toSet)
      case _ =>
    }
  }

  def createPuts(
      dataPoints: Seq[DataPoint],
      dictionaryProvider: DictionaryProvider,
      username: String
  ): Seq[(Table, Seq[Put], Seq[UpdateInterval])] = {
    val now = OffsetDateTime.now()
    dataPoints
      .groupBy(_.table)
      .map {
        case (table, points) =>
          loadDimIds(dictionaryProvider, table, points)
          val keySize = tableKeySize(table)
          val grouped = points.groupBy(rowKey(_, table, keySize, dictionaryProvider))
          val (puts, intervals) = grouped
            .map {
              case (key, dps) =>
                val baseTime = Bytes.toLong(key)
                (
                  createPutOperation(table, key, dps),
                  UpdateInterval(
                    table.name,
                    OffsetDateTime.ofInstant(Instant.ofEpochMilli(baseTime), ZoneOffset.UTC),
                    OffsetDateTime.ofInstant(Instant.ofEpochMilli(baseTime + table.rowTimeSpan), ZoneOffset.UTC),
                    now,
                    username
                  )
                )
            }
            .toSeq
            .unzip
          (table, puts, intervals.distinct)
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

  def doPutBatch(
      connection: Connection,
      dictionaryProvider: DictionaryProvider,
      namespace: String,
      username: String,
      putsBatchSize: Int,
      dataPointsBatch: Seq[DataPoint]
  ): Seq[UpdateInterval] = {
    logger.trace(s"Put ${dataPointsBatch.size} dataPoints to tsdb")
    logger.trace(s" -- DETAIL DATAPOINTS: \r\n ${dataPointsBatch.mkString("\r\n")}")

    val putsWithIntervalsByTable = createPuts(dataPointsBatch, dictionaryProvider, username)
    putsWithIntervalsByTable.flatMap {
      case (table, puts, updateIntervals) =>
        using(connection.getTable(tableName(namespace, table))) { hbaseTable =>
          puts
            .sliding(putsBatchSize, putsBatchSize)
            .foreach(putsBatch => hbaseTable.put(putsBatch.asJava))
          logger.trace(s" -- DETAIL ROWS IN TABLE ${table.name}: ${puts.length}")
          updateIntervals
        }
    }
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
    executeScan(connection, tableName(namespace, context.table), scan, context.metricsCollector, batchSize)
  }

  def executeScan(
      connection: Connection,
      table: TableName,
      scan: Scan,
      metricsCollector: MetricQueryCollector,
      batchSize: Int
  ): Iterator[Result] = {
    withIterator(connection, table, scan, metricsCollector) {
      _.iterator().asScala.grouped(batchSize)
    } {
      _.flatten
    }
  }

  def executeScan(
      connection: Connection,
      table: TableName,
      scan: Scan,
      metricsCollector: MetricQueryCollector
  ): Iterator[Result] = {
    withIterator(connection, table, scan, metricsCollector) {
      _.iterator().asScala
    } {
      identity
    }
  }

  def withIterator[R, O](
      connection: Connection,
      table: TableName,
      scan: Scan,
      metricsCollector: MetricQueryCollector
  )(getIterator: ResultScanner => Iterator[R])(finalizeResult: Iterator[R] => Iterator[O]): Iterator[O] = {

    val htable = connection.getTable(table)
    scan.setScanMetricsEnabled(metricsCollector.isEnabled)
    val scanner = htable.getScanner(scan)

    def close(): Unit = {
      scanner.close()
      htable.close()
    }

    val it = getIterator(scanner)

    val resultIterator = new AbstractIterator[R] {
      override def hasNext: Boolean = {
        metricsCollector.scan.measure(1) {
          val hasNext = it.hasNext
          if (!hasNext && scan.isScanMetricsEnabled) {
            logger.info(
              s"query_uuid: ${metricsCollector.query.id}, scans: ${scanMetricsToString(scanner.getScanMetrics)}"
            )
          }
          hasNext
        }
      }

      override def next(): R = {
        it.next()
      }
    }

    CloseableIterator(finalizeResult(resultIterator), close())
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
      Seq(Metric.Groups.default)
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
      checkTableExistsElseCreate(connection, namespace, t, config.maxRegions)
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

  private def createTable(table: Table, tableName: TableName, maxRegions: Int, admin: Admin): Unit = {
    val fieldGroups = table.metrics.map(_.group).toSet
    val families = fieldGroups map (group =>
      ColumnFamilyDescriptorBuilder
        .newBuilder(family(group))
        .setDataBlockEncoding(DataBlockEncoding.PREFIX)
        .setCompactionCompressionType(Algorithm.SNAPPY)
        .build()
    )
    val desc = TableDescriptorBuilder
      .newBuilder(tableName)
      .setColumnFamilies(families.asJavaCollection)
      .build()
    val endTime = LocalDate
      .now()
      .`with`(TemporalAdjusters.firstDayOfNextYear())
      .atStartOfDay(ZoneOffset.UTC)
      .toInstant
      .toEpochMilli
    val r = ((endTime - table.epochTime) / table.rowTimeSpan).toInt * 10
    val regions = math.min(r, maxRegions)
    admin.createTable(
      desc,
      Bytes.toBytes(baseTime(table.epochTime, table)),
      Bytes.toBytes(baseTime(endTime, table)),
      regions
    )
  }

  def checkTableExistsElseCreate(connection: Connection, namespace: String, table: Table, maxRegions: Int): Unit =
    using(connection.getAdmin) { admin =>
      val name = tableName(namespace, table)

      if (!admin.tableExists(name)) {
        createTable(table, name, maxRegions, admin)
      }
    }

  def getFirstKey(connection: Connection, tableName: TableName): Array[Byte] = {
    getFirstOrLastKey(connection, tableName, first = true)
  }

  def getLastKey(connection: Connection, tableName: TableName): Array[Byte] = {
    getFirstOrLastKey(connection, tableName, first = false)
  }

  private def getFirstOrLastKey(connection: Connection, tableName: TableName, first: Boolean): Array[Byte] = {
    val table = connection.getTable(tableName)
    val scan = new Scan().setOneRowLimit().setFilter(new FirstKeyOnlyFilter).setReversed(!first)

    using(table.getScanner(scan)) { scanner =>

      val result = scanner.next()
      if (result != null) result.getRow else Array.empty
    }
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
    metrics.getMetricsMap.asScala.map { case (k, v) => s""""$k":"$v"""" }.mkString("{", ",", "}")
  }

  def family(group: Int): Array[Byte] = s"d$group".getBytes

  def valuesByGroup(table: Table, dataPoints: Seq[DataPoint]): ValuesByGroup = {
    dataPoints.map(partitionValuesByGroup(table)).reduce(mergeMaps).map { case (k, v) => k -> v.toArray }
  }

  private def partitionValuesByGroup(table: Table)(dp: DataPoint): Map[Int, Seq[TimeShiftedValue]] = {
    val timeShift = HBaseUtils.restTime(dp.time, table)
    dp.metrics
      .groupBy(_.metric.group)
      .map { case (k, metricValues) => k -> Seq((timeShift, fieldsToBytes(table, dp.dimensions, metricValues))) }
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
      require(
        table.metricTagsSet.contains(f.metric.tag),
        s"Bad metric value $f: such metric is not defined for table ${table.name}"
      )
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
