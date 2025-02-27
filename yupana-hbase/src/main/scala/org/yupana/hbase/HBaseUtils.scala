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
import org.yupana.api.Time
import org.yupana.api.query.DataPoint
import org.yupana.api.schema._
import org.yupana.api.types.{ ID, ReaderWriter }
import org.yupana.api.utils.CloseableIterator
import org.yupana.core.TsdbConfig
import org.yupana.core.dao.DictionaryProvider
import org.yupana.core.model.{ BatchDataset, UpdateInterval }
import org.yupana.core.utils.metric.MetricQueryCollector
import org.yupana.core.utils.{ CollectionUtils, QueryUtils }
import org.yupana.serialization.{ MemoryBuffer, MemoryBufferEvalReaderWriter }

import java.nio.ByteBuffer
import java.time.temporal.TemporalAdjusters
import java.time.{ LocalDate, OffsetDateTime, ZoneOffset }
import scala.collection.{ AbstractIterator, mutable }
import scala.jdk.CollectionConverters._
import scala.collection.immutable.NumericRange
import scala.util.Using

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
  val MAX_ROW_SIZE = 2_000_000
  val tsdbSchemaTableName: String = tableNamePrefix + "table"

  implicit val readerWriter: ReaderWriter[MemoryBuffer, ID, Int, Int] = MemoryBufferEvalReaderWriter

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

  private def loadDimIds(dictionaryProvider: DictionaryProvider, table: Table, dataPoints: Seq[DataPoint]): Unit = {
    table.dimensionSeq.foreach {
      case dimension: DictionaryDimension =>
        val values = dataPoints.flatMap { dp =>
          dp.dimensionValue(dimension).filter(_.trim.nonEmpty)
        }
        dictionaryProvider.dictionary(dimension).findIdsByValues(values.toSet)
      case _ =>
    }
  }

  def doPutBatch(
      connection: Connection,
      dictionaryProvider: DictionaryProvider,
      namespace: String,
      username: String,
      dataPointsBatch: Seq[DataPoint]
  ): Seq[UpdateInterval] = {
    val r = dataPointsBatch
      .groupBy(_.table)
      .flatMap {
        case (table: Table, dps) =>
          doPutBatch(connection, dictionaryProvider, namespace, username, dps, table)
      }
      .toSeq
    r
  }

  def doPutBatch(
      connection: Connection,
      dictionaryProvider: DictionaryProvider,
      namespace: String,
      username: String,
      dataPoints: Seq[DataPoint],
      table: Table
  ): Seq[UpdateInterval] = {
    Using.resource(connection.getTable(tableName(namespace, table))) { hbaseTable =>
      val (puts, updateIntervals) =
        createPuts(dictionaryProvider, username, dataPoints, table)
      hbaseTable.put(puts.asJava)
      updateIntervals
    }
  }

  def createPuts(
      dictionaryProvider: DictionaryProvider,
      username: String,
      dataPoints: Seq[DataPoint],
      table: Table
  ): (Seq[Put], Seq[UpdateInterval]) = {

    loadDimIds(dictionaryProvider, table, dataPoints)

    val keySize = tableKeySize(table)
    val now = OffsetDateTime.now()

    val puts = mutable.Map.empty[MemoryBuffer, Put]
    val updateIntervals = mutable.Map.empty[Long, UpdateInterval]

    val buffer = MemoryBuffer.allocateHeap(MAX_ROW_SIZE)

    dataPoints.foreach { dp =>

      val time = dp.time
      val rowKey = rowKeyBuffer(dp, table, keySize, dictionaryProvider)

      table.metricGroups.foreach { group =>

        buffer.position(0)
        writeFields(table, dp.dimensions, dp.metrics, group, buffer)
        val cellBytes = Array.ofDim[Byte](buffer.position())
        buffer.get(0, cellBytes)

        val put = puts.getOrElseUpdate(rowKey, new Put(rowKey.bytes()))
        val restTime = HBaseUtils.restTime(time, table)
        val restTimeBytes = Bytes.toBytes(restTime)

        put.addColumn(family(group), restTimeBytes, cellBytes)
      }

      val baseTime = HBaseUtils.baseTime(time, table)
      updateIntervals.getOrElseUpdate(baseTime, UpdateInterval(table, baseTime, now, username))
    }
    (puts.values.toSeq, updateIntervals.values.toSeq)
  }

  def doPutBatchDataset(
      connection: Connection,
      dictionaryProvider: DictionaryProvider,
      namespace: String,
      username: String,
      batch: BatchDataset,
      table: Table
  ): Seq[UpdateInterval] = {
    Using.resource(connection.getTable(tableName(namespace, table))) { hbaseTable =>

      val keySize = tableKeySize(table)
      val now = OffsetDateTime.now()

      val buf = MemoryBuffer.allocateHeap(MAX_ROW_SIZE)
      val puts = mutable.Map.empty[MemoryBuffer, Put]
      val updateIntervals = mutable.Map.empty[Long, UpdateInterval]

      batch.foreach { rowNum =>
        val time = batch.get[Time](rowNum, "time")
        val rowKey = rowKeyBuffer(batch, rowNum, table, keySize, dictionaryProvider)
        table.metricGroups.foreach { group =>

          buf.position(0)
          writeFields(batch, rowNum, group, table, buf)

          val put = puts.getOrElseUpdate(rowKey, new Put(rowKey.bytes()))
          val restTime = HBaseUtils.restTime(time.millis, table)
          val restTimeBytes = Bytes.toBytes(restTime)
          val bytes = Array.ofDim[Byte](buf.position())
          buf.get(0, bytes)
          put.addColumn(family(group), restTimeBytes, bytes)
        }

        val baseTime = HBaseUtils.baseTime(time.millis, table)
        updateIntervals.getOrElseUpdate(baseTime, UpdateInterval(table, baseTime, now, username))
      }

      hbaseTable.put(puts.values.toList.asJava)
      updateIntervals.values.toSeq
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
      intervals: Seq[(Long, Long)],
      dimIds: Map[Dimension, Seq[_]]
  ): Option[MultiRowRangeFilter] = {
    val ranges = intervals.flatMap { case (from, to) => rowRanges(table, from, to, dimIds) }
    if (ranges.nonEmpty) {
      val filter = new MultiRowRangeFilter(new java.util.ArrayList(ranges.asJava))
      Some(filter)
    } else {
      None
    }

  }

  private def rowRanges(table: Table, from: Long, to: Long, dimIds: Map[Dimension, Seq[_]]): Seq[RowRange] = {
    val baseTimeLs = baseTimeList(from, to, table)
    val dimIdsList = dimIds.toList.map {
      case (dim, ids) =>
        ids.toList.map { id =>
          val a = Array.ofDim[Byte](dim.rStorable.size)
          val b = MemoryBuffer.ofBytes(a)
          dim.rStorable.write(b, id.asInstanceOf[dim.R]: ID[dim.R])
          a
        }
    }
    val crossJoinedDimIds = CollectionUtils.crossJoin(dimIdsList)
    val keySize = tableKeySize(table)
    for {
      time <- baseTimeLs
      cids <- crossJoinedDimIds
    } yield {
      rowRange(time, keySize, cids.toArray)
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
    val bb = MemoryBuffer.ofBytes(bytes).asSlice(TAGS_POSITION_IN_ROW_KEY, bytes.length - TAGS_POSITION_IN_ROW_KEY)
    table.dimensionSeq.foreach { dim =>
      val value = dim.rStorable.read(bb)
      dimReprs(i) = Some(value)
      i += 1
    }
    TSDRowKey(baseTime, dimReprs)
  }

  private def checkSchemaDefinition(connection: Connection, namespace: String, schema: Schema): SchemaCheckResult = {

    val metaTableName = TableName.valueOf(namespace, tsdbSchemaTableName)

    Using.resource(connection.getAdmin) { admin =>
      if (admin.tableExists(metaTableName)) {
        PersistentSchemaChecker.check(schema, readTsdbSchema(connection, namespace))
      } else {

        val tsdbSchemaBytes = PersistentSchemaChecker.toBytes(schema)
        PersistentSchemaChecker.check(schema, tsdbSchemaBytes)

        writeTsdbSchema(connection, namespace, tsdbSchemaBytes)
        Success
      }
    }
  }

  def readTsdbSchema(connection: Connection, namespace: String): Array[Byte] = {
    Using.resource(connection.getTable(TableName.valueOf(namespace, tsdbSchemaTableName))) { table =>
      val get = new Get(tsdbSchemaKey).addColumn(tsdbSchemaFamily, tsdbSchemaField)
      table.get(get).getValue(tsdbSchemaFamily, tsdbSchemaField)
    }
  }

  def writeTsdbSchema(connection: Connection, namespace: String, schemaBytes: Array[Byte]): Unit = {
    logger.info(s"Writing TSDB Schema definition to namespace $namespace")

    val metaTableName = TableName.valueOf(namespace, tsdbSchemaTableName)
    Using.resource(connection.getAdmin) { admin =>
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
      Using.resource(connection.getTable(metaTableName)) { table =>
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
      t.dimensionSeq.foreach(dictDao.checkTablesExistsElseCreate)
    }
    if (config.needCheckSchema) {
      checkSchemaDefinition(connection, namespace, schema) match {
        case Success      => logger.info("TSDB table definition checked successfully")
        case Warning(msg) => logger.warn("TSDB table definition check warnings: " + msg)
        case Error(msg)   => throw new RuntimeException("TSDB table definition check failed: " + msg)
      }
    }
  }

  def checkNamespaceExistsElseCreate(connection: Connection, namespace: String): Unit = {
    Using.resource(connection.getAdmin) { admin =>
      if (!admin.listNamespaceDescriptors.exists(_.getName == namespace)) {
        val namespaceDescriptor = NamespaceDescriptor.create(namespace).build()
        admin.createNamespace(namespaceDescriptor)
      }
    }
  }

  private def createTable(
      table: Table,
      tableName: TableName,
      maxRegions: Int,
      compressionAlgorithm: String,
      admin: Admin
  ): Unit = {
    val algorithm = Compression.getCompressionAlgorithmByName(compressionAlgorithm)
    val fieldGroups = table.metrics.map(_.group).toSet
    val families = fieldGroups map (group =>
      ColumnFamilyDescriptorBuilder
        .newBuilder(family(group))
        .setDataBlockEncoding(DataBlockEncoding.PREFIX)
        .setCompactionCompressionType(algorithm)
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

  def checkTableExistsElseCreate(connection: Connection, tableName: TableName, familyNames: Seq[Array[Byte]]): Unit = {
    try {
      Using.resource(connection.getAdmin) { admin =>
        if (!admin.tableExists(tableName)) {
          val desc = TableDescriptorBuilder
            .newBuilder(tableName)
            .setColumnFamilies(familyNames.map(ColumnFamilyDescriptorBuilder.of).asJavaCollection)
            .build()
          admin.createTable(desc)
        }
      }
    } catch {
      case _: TableExistsException =>
    }
  }

  def checkTableExistsElseCreate(
      connection: Connection,
      namespace: String,
      table: Table,
      maxRegions: Int,
      compressionAlgorithm: String
  ): Unit =
    Using.resource(connection.getAdmin) { admin =>
      val name = tableName(namespace, table)

      if (!admin.tableExists(name)) {
        createTable(table, name, maxRegions, compressionAlgorithm, admin)
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

    Using.resource(table.getScanner(scan)) { scanner =>

      val result = scanner.next()
      if (result != null) result.getRow else Array.empty
    }
  }

  private[hbase] def tableKeySize(table: Table): Int = {
    Bytes.SIZEOF_LONG + table.dimensionSeq.map(_.rStorable.size).sum
  }

  private[hbase] def rowKeyBuffer(
      dataPoint: DataPoint,
      table: Table,
      keySize: Int,
      dictionaryProvider: DictionaryProvider
  ): MemoryBuffer = {
    val bt = HBaseUtils.baseTime(dataPoint.time, table)
    val baseTimeBytes = Bytes.toBytes(bt)

    val array = Array.ofDim[Byte](keySize)
    val buffer = MemoryBuffer.ofBytes(array)

    buffer.put(baseTimeBytes)

    table.dimensionSeq.foreach {
      case dd: DictionaryDimension =>
        val id = dataPoint.dimensions
          .get(dd)
          .asInstanceOf[Option[String]]
          .filter(_.trim.nonEmpty)
          .map(v => dictionaryProvider.dictionary(dd).id(v))
          .getOrElse(NULL_VALUE)
        readerWriter.writeLong(buffer, id)

      case rd: RawDimension[_] =>
        val v = dataPoint.dimensions(rd).asInstanceOf[rd.T]
        rd.rStorable.write(buffer, v: ID[rd.T])

      case hd: HashDimension[_, _] =>
        val v = dataPoint.dimensions(hd).asInstanceOf[hd.T]
        val hash = hd.hashFunction(v).asInstanceOf[hd.R]
        hd.rStorable.write(buffer, hash: ID[hd.R])
    }

    buffer.rewind()
    buffer
  }

  private[hbase] def rowKeyBuffer(
      dataset: BatchDataset,
      rowNum: Int,
      table: Table,
      keySize: Int,
      dictionaryProvider: DictionaryProvider
  ): MemoryBuffer = {
    val time = dataset.get[Time](rowNum, "time")
    val bt = HBaseUtils.baseTime(time.millis, table)
    val buffer = MemoryBuffer.allocateNative(keySize)
    buffer.putLong(bt)

    table.dimensionSeq.foreach {
      case dd: DictionaryDimension =>
        val value = dataset.get[String](rowNum, dd.name).trim
        val id = dictionaryProvider.dictionary(dd).id(value)
        readerWriter.writeLong(buffer, id)

      case rd: RawDimension[_] =>
        val value = dataset.get[rd.dataType.T](rowNum, rd.name)(rd.dataType.internalStorable)
        rd.rStorable.write(buffer, value: ID[rd.T])

      case hd: HashDimension[_, _] =>
        val value = dataset.get[hd.T](rowNum, hd.name)(hd.dataType.internalStorable)
        val hash = hd.hashFunction(value).asInstanceOf[hd.R]
        hd.rStorable.write(buffer, hash: ID[hd.R])
    }

    buffer.rewind()
    buffer
  }

  private def scanMetricsToString(metrics: ScanMetrics): String = {
    metrics.getMetricsMap.asScala.map { case (k, v) => s""""$k":"$v"""" }.mkString("{", ",", "}")
  }

  def family(group: Int): Array[Byte] = s"d$group".getBytes

  private def writeFields(
      table: Table,
      dimensions: Map[Dimension, Any],
      metricValues: Seq[MetricValue],
      group: Int,
      buffer: MemoryBuffer
  ): Unit = {

    metricValues.foreach { f =>
      if (f.metric.group == group) {
        require(
          table.metricTagsSet.contains(f.metric.tag),
          s"Bad metric value $f: such metric is not defined for table ${table.name}"
        )
        readerWriter.writeByte(buffer, f.metric.tag)
        f.metric.dataType.storable.write(buffer, f.value: ID[f.metric.T])
      }
    }
    dimensions.foreach {
      case (d: DictionaryDimension, value) if table.dimensionTagExists(d) =>
        val tag = table.dimensionTag(d)
        readerWriter.writeByte(buffer, tag)
        d.dataType.storable.write(buffer, value.asInstanceOf[d.T]: ID[d.T])

      case (d: HashDimension[_, _], value) if table.dimensionTagExists(d) =>
        val tag = table.dimensionTag(d)
        readerWriter.writeByte(buffer, tag)
        d.tStorable.write(buffer, value.asInstanceOf[d.T]: ID[d.T])
      case _ =>
    }

  }

  private def writeFields(
      dataset: BatchDataset,
      rowNum: Int,
      group: Int,
      table: Table,
      buffer: MemoryBuffer
  ): Unit = {

    table.metrics.foreach {
      case metric if metric.group == group =>
        if (dataset.isDefined(rowNum, metric.name)) {
          val v = dataset.get(rowNum, metric.name)(metric.dataType.internalStorable)
          readerWriter.writeByte(buffer, metric.tag)
          metric.dataType.storable.write(buffer, v: ID[metric.T])
        }
      case _ =>
    }

    table.dimensionSeq.foreach {
      case dim: DictionaryDimension =>
        val tag = table.dimensionTag(dim)
        readerWriter.writeByte(buffer, tag)
        val v = dataset.get(rowNum, dim.name)(dim.dataType.internalStorable)
        dim.dataType.storable.write(buffer, v.asInstanceOf[dim.T]: ID[dim.T])

      case dim: HashDimension[_, _] =>
        val tag = table.dimensionTag(dim)
        readerWriter.writeByte(buffer, tag)
        val v = dataset.get(rowNum, dim.name)(dim.dataType.internalStorable)
        dim.tStorable.write(buffer, v.asInstanceOf[dim.T]: ID[dim.T])

      case _: RawDimension[_] =>
    }
  }
}
