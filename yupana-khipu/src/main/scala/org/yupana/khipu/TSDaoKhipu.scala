package org.yupana.khipu

import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query.{
  AndExpr,
  ConstantExpr,
  DataPoint,
  DimIdInExpr,
  DimIdNotInExpr,
  DimensionExpr,
  DimensionIdExpr,
  EqExpr,
  InExpr,
  LowerExpr,
  NeqExpr,
  NotInExpr,
  TimeExpr,
  TupleExpr
}
import org.yupana.api.schema.{ DictionaryDimension, Dimension, HashDimension, Metric, RawDimension, Schema, Table }
import org.yupana.api.types.DataType
import org.yupana.api.utils.ConditionMatchers.{
  EqString,
  EqTime,
  EqUntyped,
  GeTime,
  GtTime,
  InString,
  InTime,
  InUntyped,
  LeTime,
  LtTime,
  NeqString,
  NeqTime,
  NotInString,
  NotInTime
}
import org.yupana.api.utils.{ PrefetchedSortedSetIterator, SortedSetIterator }
import org.yupana.core.dao.TSDao
import org.yupana.core.{ ConstantCalculator, IteratorMapReducible, MapReducible }
import org.yupana.core.model.{ InternalQuery, InternalRow, InternalRowBuilder, UpdateInterval }
import org.yupana.core.utils.TimeBoundedCondition
import org.yupana.core.utils.metric.MetricQueryCollector

import java.io.File
import java.nio.ByteBuffer
import scala.collection.AbstractIterator

class TSDaoKhipu(schema: Schema) extends TSDao[Iterator, Long] {

  type IdType = Long
  type TimeFilter = Long => Boolean
  type RowFilter = InternalRow => Boolean

  val RANGE_FILTERS_LIMIT = 100000
  val CROSS_JOIN_LIMIT = 500000
  val EXTRACT_BATCH_SIZE = 10000

  val path = new File("/home/victor/tmp/yupana-khipu")
  if (!path.exists()) path.mkdirs()

  val db = new DB(path.toPath, schema)

  protected lazy val expressionCalculator: ConstantCalculator = new ConstantCalculator(schema.tokenizer)

  override val dataPointsBatchSize: Int = 50000
  val reduceLimit = 10000000

  override def mapReduceEngine(metricQueryCollector: MetricQueryCollector): MapReducible[Iterator] =
    new IteratorMapReducible(reduceLimit)

  override def putBatch(username: String)(dataPointsBatch: Seq[DataPoint]): Seq[UpdateInterval] = {

    dataPointsBatch.groupBy(_.table).foreach {
      case (table, dps) =>
        val rows = dps.map { dp =>
          val key = keyBytes(dp, table, StorageFormat.keySize(table))
          val value = valueBytes(dp, Metric.Groups.default)
          Row(key, value)
        }
        db.tables(table.name).put(rows)

    }
    Seq.empty
  }

  override def query(
      query: InternalQuery,
      internalRowBuilder: InternalRowBuilder,
      metricCollector: MetricQueryCollector
  ): Iterator[InternalRow] = {
    val tbc = TimeBoundedCondition(expressionCalculator, query.condition)

    if (tbc.size != 1) throw new IllegalArgumentException("Only one condition is supported")

    val condition = tbc.head

    val from = condition.from.getOrElse(throw new IllegalArgumentException("FROM time is not defined"))
    val to = condition.to.getOrElse(throw new IllegalArgumentException("TO time is not defined"))

    val filters = metricCollector.createDimensionFilters.measure(1) {
      val c = if (condition.conditions.nonEmpty) Some(AndExpr(condition.conditions)) else None
      createFilters(c)
    }

    val dimFilter = filters.allIncludes
    val hasEmptyFilter = dimFilter.exists(_._2.isEmpty)

    val prefetchedDimIterators: Map[Dimension, PrefetchedSortedSetIterator[_]] = dimFilter.map {
      case (d, it) =>
        val rit = it.asInstanceOf[SortedSetIterator[d.R]]
        d -> rit.prefetch(RANGE_FILTERS_LIMIT)(d.rCt)
    }.toMap

    val sizeLimitedRangeScanDims = rangeScanDimensions(query, prefetchedDimIterators)

    val rangeScanDimIds = if (hasEmptyFilter) {
      Seq.empty
    } else {
      val rangeScanDimIterators = sizeLimitedRangeScanDims.map { d =>
        (d -> prefetchedDimIterators(d)).asInstanceOf[(Dimension, PrefetchedSortedSetIterator[_])]
      }
      rangeScanDimIterators
    }

    val context = InternalQueryContext(query, metricCollector)

    val rows = executeScans(context, from, to, rangeScanDimIds, metricCollector)

    //    val includeRowFilter = prefetchedDimIterators.filter { case (d, _) => !sizeLimitedRangeScanDims.contains(d) }
    //
    //    val excludeRowFilter = filters.allExcludes.filter { case (d, _) => !sizeLimitedRangeScanDims.contains(d) }

    //    val rowFilter = createRowFilter(query.table, includeRowFilter, excludeRowFilter)
    val timeFilter = createTimeFilter(
      from,
      to,
      filters.includeTime.map(_.toSet).getOrElse(Set.empty),
      filters.excludeTime.map(_.toSet).getOrElse(Set.empty)
    )

    val mr = mapReduceEngine(metricCollector)

    val dimensions = context.table.dimensionSeq.toArray
    mr.batchFlatMap(rows, EXTRACT_BATCH_SIZE) { rs =>
      //      val filtered = context.metricsCollector.filterRows.measure(rs.size) {
      //        rs.filter(r => rowFilter(HBaseUtils.parseRowKey(r.getRow, table)))
      //      }
      metricCollector.extractDataComputation.measure(1) {
        rs.map {
          case (key, value) =>
            loadRowKey(context.table, key, dimensions, internalRowBuilder)
            loadValue(value, context, internalRowBuilder)
            internalRowBuilder.buildAndReset()
        }.filter { r =>
          timeFilter(r.get[Time](internalRowBuilder.timeIndex).millis)
        }
      }
    }
  }

  private def executeScans(
      context: InternalQueryContext,
      from: Long,
      to: Long,
      rangeScanDims: Seq[(Dimension, PrefetchedSortedSetIterator[_])],
      metricCollector: MetricQueryCollector
  ): Iterator[(Array[Byte], Array[Byte])] = {

    val ktable = db.tables(context.table.name)

    val cursor = ktable.scan()

    new AbstractIterator[(Array[Byte], Array[Byte])] {

      var isValid = cursor.next()

      override def hasNext: Boolean = isValid

      override def next(): (Array[Byte], Array[Byte]) = {
        val r = (cursor.keyBytes(), cursor.valueBytes())
        isValid = cursor.next()
        r
      }
    }
  }

  private def createTimeFilter(
      fromTime: Long,
      toTime: Long,
      includeSet: Set[Time],
      excludeSet: Set[Time]
  ): TimeFilter = {
    val baseFilter: TimeFilter = t => t >= fromTime && t < toTime
    val incMillis = includeSet.map(_.millis)
    val excMillis = excludeSet.map(_.millis)

    if (excMillis.nonEmpty) {
      if (incMillis.nonEmpty) { t =>
        baseFilter(t) && incMillis.contains(t) && !excMillis.contains(t)
      } else { t =>
        baseFilter(t) && !excMillis.contains(t)
      }
    } else {
      if (incMillis.nonEmpty) { t =>
        baseFilter(t) && incMillis.contains(t)
      } else {
        baseFilter
      }
    }
  }

  def createFilters(condition: Option[Condition]): Filters = {
    def handleEq(condition: Condition, builder: Filters.Builder): Filters.Builder = {
      condition match {
        case EqExpr(DimensionExpr(dim), ConstantExpr(c, _)) =>
          builder.includeValue(dim.aux, c.asInstanceOf[dim.T])

        case EqExpr(ConstantExpr(c, _), DimensionExpr(dim)) =>
          builder.includeValue(dim.aux, c.asInstanceOf[dim.T])

        case EqString(LowerExpr(DimensionExpr(dim)), ConstantExpr(c, _)) =>
          builder.includeValue(dim.aux, c.asInstanceOf[dim.T])

        case EqString(ConstantExpr(c, _), LowerExpr(DimensionExpr(dim))) =>
          builder.includeValue(dim.aux, c.asInstanceOf[dim.T])

        case EqString(DimensionIdExpr(dim), ConstantExpr(c, _)) =>
          builder.includeIds(dim.aux, dimIdValueFromString(dim.aux, c).toSeq)

        case EqString(ConstantExpr(c, _), DimensionIdExpr(dim)) =>
          builder.includeIds(dim.aux, dimIdValueFromString(dim.aux, c).toSeq)

        case EqTime(TimeExpr, ConstantExpr(c, _)) =>
          builder.includeTime(c)

        case EqTime(ConstantExpr(c, _), TimeExpr) =>
          builder.includeTime(c)

        case EqUntyped(t: TupleExpr[a, b], ConstantExpr(v: (_, _), _)) =>
          val filters1 = createFilters(InExpr(t.e1, Set(v._1).asInstanceOf[Set[a]]), builder)
          createFilters(InExpr(t.e2, Set(v._2).asInstanceOf[Set[b]]), filters1)

        case EqUntyped(ConstantExpr(v: (_, _), _), t: TupleExpr[a, b]) =>
          val filters1 = createFilters(InExpr(t.e1, Set(v._1).asInstanceOf[Set[a]]), builder)
          createFilters(InExpr(t.e2, Set(v._2).asInstanceOf[Set[b]]), filters1)

        case _ => builder
      }
    }

    def handleNeq(condition: Condition, builder: Filters.Builder): Filters.Builder = {
      condition match {
        case NeqExpr(DimensionExpr(dim), ConstantExpr(c, _)) =>
          builder.excludeValue(dim.aux, c.asInstanceOf[dim.T])

        case NeqExpr(ConstantExpr(c, _), DimensionExpr(dim)) =>
          builder.excludeValue(dim.aux, c.asInstanceOf[dim.T])

        case NeqString(LowerExpr(DimensionExpr(dim)), ConstantExpr(c, _)) =>
          builder.excludeValue(dim.aux, c.asInstanceOf[dim.T])

        case NeqString(ConstantExpr(c, _), LowerExpr(DimensionExpr(dim))) =>
          builder.excludeValue(dim.aux, c.asInstanceOf[dim.T])

        case NeqString(DimensionIdExpr(dim), ConstantExpr(c, _)) =>
          builder.excludeIds(dim.aux, dimIdValueFromString(dim.aux, c).toSeq)

        case NeqString(ConstantExpr(c, _), DimensionIdExpr(dim)) =>
          builder.excludeIds(dim.aux, dimIdValueFromString(dim.aux, c).toSeq)

        case NeqTime(TimeExpr, ConstantExpr(c, _)) =>
          builder.excludeTime(c)

        case NeqTime(ConstantExpr(c, _), TimeExpr) =>
          builder.excludeTime(c)

        case _ => builder
      }
    }

    def handleIn(condition: Condition, builder: Filters.Builder): Filters.Builder = {
      condition match {
        case InExpr(DimensionExpr(dim), consts) =>
          builder.includeValues(dim, consts)

        case InString(LowerExpr(DimensionExpr(dim)), consts) =>
          builder.includeValues(
            dim,
            consts.asInstanceOf[Set[dim.T]]
          )

        case InTime(TimeExpr, consts) =>
          builder.includeTime(consts.asInstanceOf[Set[Time]])

        case InString(DimensionIdExpr(dim), dimIds) =>
          builder.includeIds(
            dim.aux,
            dimIds.toSeq.flatMap(v => dimIdValueFromString(dim.aux, v))
          )

        case InUntyped(t: TupleExpr[a, b], vs) =>
          val filters1 = createFilters(InExpr(t.e1, vs.asInstanceOf[Set[(a, b)]].map(_._1)), builder)
          createFilters(InExpr(t.e2, vs.asInstanceOf[Set[(a, b)]].map(_._2)), filters1)

        case _ => builder
      }
    }

    def handleNotIn(condition: Condition, builder: Filters.Builder): Filters.Builder = {
      condition match {
        case NotInExpr(DimensionExpr(dim), consts) =>
          builder.excludeValues(dim, consts.asInstanceOf[Set[dim.T]])

        case NotInString(LowerExpr(DimensionExpr(dim)), consts) =>
          builder.excludeValues(dim, consts.asInstanceOf[Set[dim.T]])

        case NotInString(DimensionIdExpr(dim), dimIds) =>
          builder.excludeIds(
            dim.aux,
            dimIds.toSeq.flatMap(v => dimIdValueFromString(dim.aux, v))
          )

        case NotInTime(TimeExpr, consts) =>
          builder.excludeTime(consts)

        case _ => builder
      }
    }

    def createFilters(condition: Condition, builder: Filters.Builder): Filters.Builder = {
      condition match {
        case EqExpr(_, _) => handleEq(condition, builder)

        case NeqExpr(_, _) => handleNeq(condition, builder)

        case InExpr(_, _) => handleIn(condition, builder)

        case NotInExpr(_, _) => handleNotIn(condition, builder)

        case DimIdInExpr(dim, dimIds) =>
          builder.includeIds(dim, dimIds)

        case DimIdNotInExpr(dim, dimIds) =>
          builder.excludeIds(dim, dimIds)

        case AndExpr(conditions) =>
          conditions.foldLeft(builder)((f, c) => createFilters(c, f))

        case _ => builder
      }
    }

    condition match {
      case Some(c) =>
        createFilters(c, Filters.newBuilder).build(valuesToIds)

      case None =>
        Filters.empty
    }
  }

  override def isSupportedCondition(condition: Condition): Boolean = {
    def handleEq(condition: EqExpr[_]): Boolean = {
      condition match {
        case EqTime(TimeExpr, ConstantExpr(_, _))                         => true
        case EqTime(ConstantExpr(_, _), TimeExpr)                         => true
        case EqExpr(_: DimensionExpr[_], ConstantExpr(_, _))              => true
        case EqExpr(ConstantExpr(_, _), _: DimensionExpr[_])              => true
        case EqString(LowerExpr(_: DimensionExpr[_]), ConstantExpr(_, _)) => true
        case EqString(ConstantExpr(_, _), LowerExpr(_: DimensionExpr[_])) => true
        case EqString(_: DimensionIdExpr, ConstantExpr(_, _))             => true
        case EqString(ConstantExpr(_, _), _: DimensionIdExpr)             => true
        case _                                                            => false
      }
    }

    def handleNeq(condition: NeqExpr[_]): Boolean = {
      condition match {
        case NeqTime(TimeExpr, ConstantExpr(_, _))                         => true
        case NeqTime(ConstantExpr(_, _), TimeExpr)                         => true
        case NeqExpr(_: DimensionExpr[_], ConstantExpr(_, _))              => true
        case NeqExpr(ConstantExpr(_, _), _: DimensionExpr[_])              => true
        case NeqString(LowerExpr(_: DimensionExpr[_]), ConstantExpr(_, _)) => true
        case NeqString(LowerExpr(ConstantExpr(_, _)), _: DimensionExpr[_]) => true
        case NeqString(_: DimensionIdExpr, ConstantExpr(_, _))             => true
        case NeqString(ConstantExpr(_, _), _: DimensionIdExpr)             => true
        case _                                                             => false
      }
    }

    condition match {
      case e: EqExpr[_]                                   => handleEq(e)
      case e: NeqExpr[_]                                  => handleNeq(e)
      case GtTime(TimeExpr, ConstantExpr(_, _))           => true
      case GtTime(ConstantExpr(_, _), TimeExpr)           => true
      case LtTime(TimeExpr, ConstantExpr(_, _))           => true
      case LtTime(ConstantExpr(_, _), TimeExpr)           => true
      case GeTime(TimeExpr, ConstantExpr(_, _))           => true
      case GeTime(ConstantExpr(_, _), TimeExpr)           => true
      case LeTime(TimeExpr, ConstantExpr(_, _))           => true
      case LeTime(ConstantExpr(_, _), TimeExpr)           => true
      case InTime(TimeExpr, _)                            => true
      case NotInTime(TimeExpr, _)                         => true
      case _: DimIdInExpr[_, _]                           => true
      case _: DimIdNotInExpr[_, _]                        => true
      case InExpr(_: DimensionExpr[_], _)                 => true
      case NotInExpr(_: DimensionExpr[_], _)              => true
      case InString(LowerExpr(_: DimensionExpr[_]), _)    => true
      case NotInString(LowerExpr(_: DimensionExpr[_]), _) => true
      case InString(_: DimensionIdExpr, _)                => true
      case NotInString(_: DimensionIdExpr, _)             => true
      case _                                              => false
    }
  }

  private def valuesToIds(
      dimension: DictionaryDimension,
      values: SortedSetIterator[String]
  ): SortedSetIterator[Long] = {
    val it = Iterator.empty
    SortedSetIterator(it)
  }

  private def dimIdValueFromString[R](dim: Dimension.Aux2[_, R], value: String): Option[R] = {
    //    Try {
    //      val bytes = javax.xml.bind.DatatypeConverter.parseHexBinary(value)
    //      dim.rStorable.read(bytes)
    //    }.toOption
    None
  }

  private def rangeScanDimensions(
      query: InternalQuery,
      prefetchedDimIterators: Map[Dimension, PrefetchedSortedSetIterator[_]]
  ): Seq[Dimension] = {

    val continuousDims = query.table.dimensionSeq.takeWhile(prefetchedDimIterators.contains)
    val sizes = continuousDims
      .scanLeft(1L) {
        case (size, dim) =>
          val it = prefetchedDimIterators(dim)
          val itSize = if (it.isAllFetched) it.fetched.length else CROSS_JOIN_LIMIT
          size * itSize
      }
      .drop(1)

    val sizeLimitedRangeScanDims = continuousDims.zip(sizes).takeWhile(_._2 <= CROSS_JOIN_LIMIT).map(_._1)
    sizeLimitedRangeScanDims
  }

  private def keyBytes(
      dataPoint: DataPoint,
      table: Table,
      keySize: Int
  ): Array[Byte] = {
    val bt = baseTime(dataPoint.time, table)

    val buffer = ByteBuffer
      .allocate(keySize)
      .putLong(bt)

    table.dimensionSeq.foreach { dim =>
      val bytes = dim match {
        case _: DictionaryDimension =>
          Array.ofDim[Byte](java.lang.Long.BYTES)

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

    buffer.putLong(restTime(dataPoint.time, table))

    buffer.array()
  }

  private def loadRowKey(
      table: Table,
      rowKey: Array[Byte],
      dimensions: Array[Dimension],
      internalRowBuilder: InternalRowBuilder
  ): Unit = {
    val bb = ByteBuffer.wrap(rowKey)

    val baseTime = bb.getLong()

    var i = 0

    dimensions.foreach { dim =>
      bb.mark()

      val value = dim.rStorable.read(bb)
      if (dim.isInstanceOf[RawDimension[_]]) {
        internalRowBuilder.set((Table.DIM_TAG_OFFSET + i).toByte, value)
      }
      //      if (internalRowBuilder.needId((Table.DIM_TAG_OFFSET + i).toByte)) {
      //        val bytes = new Array[Byte](dim.rStorable.size)
      //        bb.reset()
      //        bb.get(bytes)
      //        internalRowBuilder.setId((Table.DIM_TAG_OFFSET + i).toByte, new String(Hex.encodeHex(bytes)))
      //      }

      i += 1
    }
    val currentTime = bb.getLong
    internalRowBuilder.set(Time(baseTime + currentTime))
  }

  def baseTime(time: Long, table: Table): Long = {
    time - time % table.rowTimeSpan
  }

  def restTime(time: Long, table: Table): Long = {
    time % table.rowTimeSpan
  }

  def valueBytes(dp: DataPoint, group: Int) = {

    val metricFieldBytes = dp.metrics.collect {
      case f if f.metric.group == group =>
        val bytes = f.metric.dataType.storable.write(f.value)
        (f.metric.tag, bytes)
    }

    val dimensionFieldBytes = dp.dimensions.collect {
      case (d: DictionaryDimension, value) if dp.table.dimensionTagExists(d) =>
        val tag = dp.table.dimensionTag(d)
        val bytes = d.dataType.storable.write(value.asInstanceOf[d.T])
        (tag, bytes)
      case (d: HashDimension[_, _], value) if dp.table.dimensionTagExists(d) =>
        val tag = dp.table.dimensionTag(d)
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

  private def loadValue(
      bytes: Array[Byte],
      context: InternalQueryContext,
      internalRowBuilder: InternalRowBuilder
  ): Unit = {
    val bb = ByteBuffer.wrap(bytes)
    while (bb.hasRemaining) {
      val tag = bb.get()
      context.table.fieldForTag(tag) match {
        case Some(Left(metric)) =>
          val v = metric.dataType.storable.read(bb)
          internalRowBuilder.set(tag, v)
        case Some(Right(_: DictionaryDimension)) =>
          val v = DataType.stringDt.storable.read(bb)
          internalRowBuilder.set(tag, v)
        case Some(Right(hd: HashDimension[_, _])) =>
          val v = hd.tStorable.read(bb)
          internalRowBuilder.set(tag, v)
        case _ =>
          throw new IllegalStateException(
            s"Unknown tag: $tag [${context.table.fieldForTag(tag)}] , in table: ${context.table.name}"
          )
      }
    }
  }
}
