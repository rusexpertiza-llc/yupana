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

package org.yupana.khipu

import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.schema.{ DictionaryDimension, Dimension, HashDimension, Metric, RawDimension, Schema, Table }
import org.yupana.api.types.{ ID, ReaderWriter, TypedInt }
import org.yupana.api.utils.ConditionMatchers._
import org.yupana.api.utils.{ PrefetchedSortedSetIterator, SortedSetIterator }
import org.yupana.core.dao.TSDao
import org.yupana.core.{ ConstantCalculator, IteratorMapReducible, MapReducible, QueryContext }
import org.yupana.core.model.{ InternalQuery, InternalRow, InternalRowBuilder, UpdateInterval }
import org.yupana.core.utils.{ CollectionUtils, FlatAndCondition }
import org.yupana.core.utils.metric.MetricQueryCollector
import org.yupana.khipu.storage.{ Cursor, DB, KTable, Prefix, Row, StorageFormat }
import org.yupana.readerwriter.{ ByteBufferEvalReaderWriter, MemoryBuffer, MemoryBufferEvalReaderWriter }
import org.yupana.settings.Settings

import java.io.File
import java.nio.ByteBuffer
import scala.collection.AbstractIterator

class TSDaoKhipu(schema: Schema, settings: Settings) extends TSDao[Iterator, Long] {

  type IdType = Long
  type TimeFilter = Long => Boolean
  type RowFilter = InternalRow => Boolean

  val RANGE_FILTERS_LIMIT = 100000
  val CROSS_JOIN_LIMIT = 500000
  val EXTRACT_BATCH_SIZE = 10000

  val path = new File(settings[String]("yupana.khipu.storage.path"))
  if (!path.exists()) path.mkdirs()

  val db = new DB(path.toPath, schema)

  implicit private val readerWriter: ReaderWriter[MemoryBuffer, ID, TypedInt] = MemoryBufferEvalReaderWriter

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
      queryContext: QueryContext,
      metricCollector: MetricQueryCollector
  ): Iterator[InternalRow] = {

    val mr = mapReduceEngine(metricCollector)

    val conditionByTime = FlatAndCondition.mergeByTime(query.condition)
    val flatAndConditions = conditionByTime.flatMap(_._3)
    val squashedCondition = OrExpr(flatAndConditions)

    val squashedFilters = metricCollector.createDimensionFilters.measure(1) {
      createFilters(Some(squashedCondition))
    }

    val intervals = conditionByTime.flatMap {
      case (from, to, _) =>
        calculateTimeIntervals(query.table, from, to, squashedFilters.includeTime, squashedFilters.excludeTime)
    }

    val dimFilter = squashedFilters.allIncludes
    val hasEmptyFilter = dimFilter.exists(_._2.isEmpty)

    val prefetchedDimIterators: Map[Dimension, PrefetchedSortedSetIterator[_]] = dimFilter.map {
      case (d, it) =>
        val rit = it.asInstanceOf[SortedSetIterator[d.R]]
        d -> rit.prefetch(RANGE_FILTERS_LIMIT)(d.rCt)
    }

    val sizeLimitedRangeScanDims = rangeScanDimensions(query, prefetchedDimIterators)

    if (hasEmptyFilter) {
      mr.empty[InternalRow]
    } else {

      val rangeScanDimIterators = sizeLimitedRangeScanDims.map { d =>
        (d -> prefetchedDimIterators(d)).asInstanceOf[(Dimension, PrefetchedSortedSetIterator[_])]
      }

//      val rowPostFilter: RowFilter = if (flatAndConditions.distinct.size == 1) {
//        val includeRowFilter = prefetchedDimIterators.filter { case (d, _) => !sizeLimitedRangeScanDims.contains(d) }
//
//        val excludeRowFilter = squashedFilters.allExcludes.filter {
//          case (d, _) => !sizeLimitedRangeScanDims.contains(d)
//        }
//
//        createRowFilter(query.table, includeRowFilter, excludeRowFilter, internalRowBuilder)
//
//      } else { _ => true }

//      val timeFilter = createTimeFilter(
//        intervals,
//        squashedFilters.includeTime.getOrElse(Set.empty),
//        squashedFilters.excludeTime.getOrElse(Set.empty)
//      )

//      val context = InternalQueryContext(query, metricCollector)

      val cursor = executeScans(query.table, intervals, rangeScanDimIterators, metricCollector)
      val calc = queryContext.calculator

      new AbstractIterator[InternalRow] {

        private var isValid = cursor.next()

        override def hasNext: Boolean = isValid

        override def next(): InternalRow = {
          val memBuf = cursor.row().asMemoryBuffer()
          val row = calc.evaluateReadRow(memBuf, internalRowBuilder)
          isValid = cursor.next()
          row
        }
      }
    }

    //            .filter { r =>
//            timeFilter(r.get[Time](internalRowBuilder.timeIndex, internalRowBuilder).millis) &&
//            rowPostFilter(r)
//          }

  }

  private def baseTimeList(fromTime: Long, toTime: Long, table: Table): Seq[Long] = {
    val startBaseTime = fromTime - (fromTime % table.rowTimeSpan)
    val stopBaseTime = toTime - (toTime % table.rowTimeSpan)
    startBaseTime to stopBaseTime by table.rowTimeSpan
  }

  private def executeScans(
      table: Table,
      intervals: Seq[(Long, Long)],
      rangeScanDims: Seq[(Dimension, PrefetchedSortedSetIterator[_])],
      metricCollector: MetricQueryCollector
  ): Cursor = {

    implicit val rw = ByteBufferEvalReaderWriter
    val prefixes = if (rangeScanDims.nonEmpty) {
      val dimBytes = rangeScanDims.map {
        case (dim, ids) =>
          ids.toList
            .map { id =>
              val arr = Array.ofDim[Byte](dim.rStorable.size)
              val bb = ByteBuffer.wrap(arr)
              val v: ID[dim.R] = id.asInstanceOf[dim.R]
              dim.rStorable.write(bb, v)
              bb.array()
            }

      }

      val crossJoinedDims = CollectionUtils.crossJoin(dimBytes.toList)

      for {
        (from, to) <- intervals
        baseTime <- baseTimeList(from, to, table)
        dimValues <- crossJoinedDims
      } yield {
        Prefix(baseTime, dimValues)
      }
    } else {
      Seq.empty
    }

    val ktable = db.tables(table.name)

    ktable.scan(SortedSetIterator(prefixes.iterator))

    //      val dimensions = query.table.dimensionSeq.toArray

  }

  private def calculateTimeIntervals(
      table: Table,
      from: Long,
      to: Long,
      includeTime: Option[Set[Time]],
      excludeTime: Option[Set[Time]]
  ): Seq[(Long, Long)] = {

    includeTime match {
      case None =>
        Seq((from, to))

      case Some(inc) =>
        val timePoints = (inc.map(_.millis) -- excludeTime.map(_.map(_.millis)).getOrElse(Set.empty)).toSeq

        val intervals = timePoints
          .groupBy { t =>
            val s = baseTime(t, table)
            s -> (s + table.rowTimeSpan)
          }
          .map { case (k, vs) => k -> vs.sorted }
          .toSeq

        intervals
          .sortBy(_._1._1)
          .foldRight(List.empty[((Long, Long), Seq[Long])]) {
            case (i @ ((iFrom, iTo), iVs), (x @ ((xFrom, xTo), xVs)) :: xs) =>
              if (xFrom == iTo) ((iFrom, xTo), iVs ++ xVs) :: xs else i :: x :: xs
            case (i, Nil) => i :: Nil
          }
          .map(x => x._2.head -> (x._2.last + 1))
    }
  }

  def createTimeFilter(
      intervals: Seq[(Long, Long)],
      includeSet: Set[Time],
      excludeSet: Set[Time]
  ): TimeFilter = {
    val baseFilter: TimeFilter = t => intervals.exists { case (from, to) => t >= from && t < to }
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
          builder.includeValue(dim.aux, c)

        case EqExpr(ConstantExpr(c, _), DimensionExpr(dim)) =>
          builder.includeValue(dim.aux, c)

        case EqString(LowerExpr(DimensionExpr(dim)), ConstantExpr(c, _)) =>
          builder.includeValue(dim.aux, c)

        case EqString(ConstantExpr(c, _), LowerExpr(DimensionExpr(dim))) =>
          builder.includeValue(dim.aux, c)

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
          builder.excludeValue(dim.aux, c)

        case NeqExpr(ConstantExpr(c, _), DimensionExpr(dim)) =>
          builder.excludeValue(dim.aux, c)

        case NeqString(LowerExpr(DimensionExpr(dim)), ConstantExpr(c, _)) =>
          builder.excludeValue(dim.aux, c)

        case NeqString(ConstantExpr(c, _), LowerExpr(DimensionExpr(dim))) =>
          builder.excludeValue(dim.aux, c)

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
          builder.includeValues(dim, consts)

        case InTime(TimeExpr, consts) =>
          builder.includeTime(consts)

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
          builder.excludeValues(dim, consts)

        case NotInString(LowerExpr(DimensionExpr(dim)), consts) =>
          builder.excludeValues(dim, consts)

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

        case OrExpr(conditions) =>
          conditions.map(c => createFilters(c, Filters.newBuilder)).foldLeft(builder)(_ union _)

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

  def createRowFilter(
      table: Table,
      include: Map[Dimension, SortedSetIterator[_]],
      exclude: Map[Dimension, SortedSetIterator[_]],
      internalRowBuilder: InternalRowBuilder
  ): RowFilter = {

    val includeMap: Map[Dimension, Set[Any]] = include.map { case (k, v) => k -> v.toSet }
    val excludeMap: Map[Dimension, Set[Any]] = exclude.map { case (k, v) => k -> v.toSet }

    if (excludeMap.nonEmpty) {
      if (includeMap.nonEmpty) {
        rowFilter(table, internalRowBuilder)((dim, x) =>
          includeMap.get(dim).forall(_.contains(x)) && !excludeMap.get(dim).exists(_.contains(x))
        )
      } else {
        rowFilter(table, internalRowBuilder)((dim, x) => !excludeMap.get(dim).exists(_.contains(x)))
      }
    } else {
      if (includeMap.nonEmpty) {
        rowFilter(table, internalRowBuilder)((dim, x) => includeMap.get(dim).forall(_.contains(x)))
      } else { _ =>
        true
      }
    }
  }

  private def rowFilter(table: Table, internalRowBuilder: InternalRowBuilder)(
      f: (Dimension, Any) => Boolean
  ): RowFilter = { row =>
    var i = 0
    table.dimensionSeq.forall { dim =>
      val idx = internalRowBuilder.tagIndex((Table.DIM_TAG_OFFSET + i).toByte)
      val v = row.get[dim.T](internalRowBuilder, idx)(dim.dataType.internalStorable)
      val r = f(dim, v)
      i += 1
      r
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

    val array = Array.ofDim[Byte](keySize)
    val buffer = MemoryBuffer.ofBytes(array)

    buffer.putLong(bt)

    table.dimensionSeq.foreach { dim =>
      val bytes = dim match {
        case _: DictionaryDimension =>
          Array.ofDim[Byte](java.lang.Long.BYTES)

        case rd: RawDimension[_] =>
          val arr = Array.ofDim[Byte](dim.rStorable.size)
          val bb = MemoryBuffer.ofBytes(arr)
          val v: ID[rd.T] = dataPoint.dimensions(dim).asInstanceOf[rd.T]
          rd.rStorable.write(bb, v)
          arr

        case hd: HashDimension[_, _] =>
          val arr = Array.ofDim[Byte](hd.rStorable.size)
          val bb = MemoryBuffer.ofBytes(arr)
          val v = dataPoint.dimensions(dim).asInstanceOf[hd.T]
          val hash: ID[hd.R] = hd.hashFunction(v).asInstanceOf[hd.R]
          hd.rStorable.write(bb, hash)
          arr
      }

      buffer.put(bytes)
    }

    buffer.putLong(restTime(dataPoint.time, table))

    array
  }

//  private def loadRowKey(
//      cursor: Cursor,
//      dimensions: Array[Dimension],
//      internalRowBuilder: InternalRowBuilder
//  ): Unit = {
//
//    implicit val rw = EvalReaderWriter
//
//    val bb = cursor.key().asByteBuffer()
//    val baseTime = bb.getLong()
//
//    var i = 0
//
//    dimensions.foreach { dim =>
//      bb.mark()
//
//      val value = dim.rStorable.read(bb)
//      if (dim.isInstanceOf[RawDimension[_]]) {
//        internalRowBuilder.set((Table.DIM_TAG_OFFSET + i).toByte, value)
//      }
//      //      if (internalRowBuilder.needId((Table.DIM_TAG_OFFSET + i).toByte)) {
//      //        val bytes = new Array[Byte](dim.rStorable.size)
//      //        bb.reset()
//      //        bb.get(bytes)
//      //        internalRowBuilder.setId((Table.DIM_TAG_OFFSET + i).toByte, new String(Hex.encodeHex(bytes)))
//      //      }
//
//      i += 1
//    }
//    val currentTime = bb.getLong
//    internalRowBuilder.set(Time(baseTime + currentTime))
//  }

  def baseTime(time: Long, table: Table): Long = {
    time - time % table.rowTimeSpan
  }

  def restTime(time: Long, table: Table): Long = {
    time % table.rowTimeSpan
  }

  def valueBytes(dp: DataPoint, group: Int): Array[Byte] = {
    val bb = MemoryBuffer.allocateHeap(KTable.MAX_ROW_SIZE)

    dp.metrics.foreach {
      case f if f.metric.group == group =>
        bb.put(f.metric.tag)
        f.metric.dataType.storable.write(bb, f.value: ID[f.metric.T])
      case _ =>
    }

    dp.dimensions.foreach {
      case (d: DictionaryDimension, value) if dp.table.dimensionTagExists(d) =>
        val tag = dp.table.dimensionTag(d)
        bb.put(tag)
        d.dataType.storable.write(bb, value.asInstanceOf[d.T]: ID[d.T])
      case (d: HashDimension[_, _], value) if dp.table.dimensionTagExists(d) =>
        val tag = dp.table.dimensionTag(d)
        bb.put(tag)
        d.tStorable.write(bb, value.asInstanceOf[d.T]: ID[d.T])
      case _ =>
    }

    val size = bb.position()
    val res = Array.ofDim[Byte](size)
    bb.rewind()
    bb.get(res)
    res
  }

//  private def loadValue(
//      cursor: Cursor,
//      context: InternalQueryContext,
//      internalRowBuilder: InternalRowBuilder
//  ): Unit = {
//    val bb = cursor.value().asByteBuffer()
//
//    while (bb.hasRemaining) {
//      val tag = bb.get()
//      context.table.fieldForTag(tag) match {
//        case Some(Left(metric)) =>
//          val v = metric.dataType.storable.read(bb)
//          internalRowBuilder.set(tag, v)
//        case Some(Right(_: DictionaryDimension)) =>
//          val v = DataType.stringDt.storable.read(bb)
//          internalRowBuilder.set(tag, v)
//        case Some(Right(hd: HashDimension[_, _])) =>
//          val v = hd.tStorable.read(bb)
//          internalRowBuilder.set(tag, v)
//        case _ =>
//          throw new IllegalStateException(
//            s"Unknown tag: $tag [${context.table.fieldForTag(tag)}] , in table: ${context.table.name}"
//          )
//      }
//    }
//  }
}
