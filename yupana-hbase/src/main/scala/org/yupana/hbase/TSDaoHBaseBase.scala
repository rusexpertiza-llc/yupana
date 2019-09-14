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

import java.nio.ByteBuffer
import java.util

import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.{FilterList, FuzzyRowFilter, MultiRowRangeFilter}
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange
import org.apache.hadoop.hbase.util.Bytes
import org.yupana.api.Time
import org.yupana.api.query._
import org.yupana.api.schema.{Dimension, Metric, Table}
import org.yupana.core.MapReducible
import org.yupana.core.dao._
import org.yupana.core.model.{InternalQuery, InternalRow, InternalRowBuilder}
import org.yupana.core.utils.metric.MetricQueryCollector
import org.yupana.core.utils.{CollectionUtils, SparseTable, TimeBoundedCondition}
import org.yupana.hbase.Filtration.TimeFilter
import org.yupana.math.KMeans

import scala.collection.immutable.NumericRange
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.language.higherKinds

trait TSDaoHBaseBase[Collection[_]] extends TSReadingDao[Collection, Long] with StrictLogging {
  type IdType = Long

  import org.yupana.core.utils.ConditionMatchers.{Equ, Neq}

  val TIME = Dimension("time")

  val QUERY_TAG_LIMIT = 100000
  val DISCRETE_TAG_FILTERS_LIMIT = 20

  def mr: MapReducible[Collection]
  def dictionaryProvider: DictionaryProvider

  case class Filters(includeTags: DimensionFilter[IdType],
                     excludeTags: DimensionFilter[IdType],
                     includeTime: DimensionFilter[Time],
                     excludeTime: DimensionFilter[Time],
                     condition: Option[Condition]
                    )

  def executeScans(table: Table, scans: Seq[Scan], metricCollector: MetricQueryCollector): Collection[TSDOutputRow[IdType]]

  override def query(query: InternalQuery, valueDataBuilder: InternalRowBuilder, metricCollector: MetricQueryCollector): Collection[InternalRow] = {

    val tbc = TimeBoundedCondition(query.condition)

    if (tbc.size != 1) throw new IllegalArgumentException("Only one condition is supported")

    val condition = tbc.head

    val from = condition.from.getOrElse(throw new IllegalArgumentException("FROM time is not defined"))
    val to = condition.to.getOrElse(throw new IllegalArgumentException("TO time is not defined"))

    val filters = metricCollector.createQueriesTags.measure {
      val c = if (condition.conditions.nonEmpty) Some(And(condition.conditions)) else None
      createFilters(c)
    }

    val (scans, timeFilters, rowFilters) =
      createScansAndFilters(query, from, to, filters.includeTags exclude filters.excludeTags, filters.includeTime exclude filters.excludeTime)

    val rows = executeScans(query.table, scans, metricCollector)

    val rowFilter = createRowFilter(query.table, rowFilters, filters.excludeTags.toMap)

    val filtered = mr.filter(rows)(rowFilter)

    val schemaContext = SchemaContext(query)
    val timeFilter = createTimeFilter(from, to, timeFilters.getOrElse(TIME, Set.empty).map(_.millis), filters.excludeTime.toMap.getOrElse(TIME, Set.empty).map(_.millis))
    mr.batchFlatMap(filtered)(10000, rs =>
      extractDataWithMetric(schemaContext, valueDataBuilder, rs, timeFilter, metricCollector)
    )
  }

  def createScansAndFilters(query: InternalQuery,
                            from: Long,
                            to: Long,
                            tagFilters: DimensionFilter[IdType],
                            timeFilters: DimensionFilter[Time]):
    (Seq[Scan], Map[Dimension, Set[Time]], Map[Dimension, Set[IdType]]) = {

    (tagFilters, timeFilters) match {
      case (NoResult(), _) =>
        (Seq.empty, Map.empty[Dimension, Set[Time]], Map.empty[Dimension, Set[IdType]])

      case (_, NoResult()) =>
        (Seq.empty, Map.empty[Dimension, Set[Time]], Map.empty[Dimension, Set[IdType]])

      case (byDimension, byTime) =>
        val (multiRowRangeFilter, hashedDimRowFilter) = rangeScanFilters(query, from, to, byDimension.toMap)
        val (hbaseFuzzyRowFilter, discDimRowFilter) = discreteDimensionFilters(query, byDimension.toMap)
        (Seq(createScan(query, multiRowRangeFilter, hbaseFuzzyRowFilter)), byTime.toMap, discDimRowFilter ++ hashedDimRowFilter)
    }
  }

  def createScan(query: InternalQuery, multiRowRangeFilter: MultiRowRangeFilter, hbaseFuzzyRowFilter: Seq[FuzzyRowFilter]): Scan = {
    val start = multiRowRangeFilter.getRowRanges.asScala.head.getStartRow
    val stop = multiRowRangeFilter.getRowRanges.asScala.toList.last.getStopRow

    val hbaseFilter =
      if (hbaseFuzzyRowFilter.nonEmpty) {
        val orFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE, hbaseFuzzyRowFilter: _*)
        new FilterList(FilterList.Operator.MUST_PASS_ALL, multiRowRangeFilter, orFilter)
      } else {
        multiRowRangeFilter
      }

    val scan = new Scan(start, stop).setFilter(hbaseFilter)
    familiesQueried(query).foreach(f => scan.addFamily(HBaseUtils.family(f)))
    scan
  }

  private def rangeScanFilters(query: InternalQuery, from: IdType, to: IdType, dimensionFilters: Map[Dimension, Set[IdType]]): (MultiRowRangeFilter, Map[Dimension, Set[IdType]]) = {
    val rangeScanDims = rangeScanDimensions(query, dimensionFilters)

    val startBaseTime = from - (from % query.table.rowTimeSpan)
    val stopBaseTime = to - (to % query.table.rowTimeSpan)
    val baseTimeList = startBaseTime to stopBaseTime by query.table.rowTimeSpan

    val hashedDimension = rangeScanDims.lastOption.filter(_.hashFunction.isDefined)
    val lastDimRanges = hashedDimension
      .map(hd => hashedDimensionRanges(hd, rangeScanDims, dimensionFilters))
      .orElse(rangeScanDims.lastOption.map(d => dimensionFilters(d).map(v => (v, v))))
      .getOrElse(Set.empty)

    logger.debug(s"Last range scan dimension ranges: ${lastDimRanges.size} for ${rangeScanDims.lastOption.map(d => dimensionFilters(d).size)} dimension values")

    val prefixes = rangeScanDims.dropRight(1).map(d => dimensionFilters(d))
    val hbaseRowRanges = rowRanges(baseTimeList, prefixes, lastDimRanges)
    logger.debug("Total row ranges: " + hbaseRowRanges.size)
    val multiRowRangeFilter = new MultiRowRangeFilter(new util.ArrayList(hbaseRowRanges.asJava))

    val hashedDimRowFilter = rangeScanDims.lastOption.map { dim =>
      if (lastDimRanges.exists(v => v._1 != v._2)) {
        Map(dim -> dimensionFilters(dim))
      } else {
        Map.empty[Dimension, Set[IdType]]
      }
    }.getOrElse(Map.empty)

    (multiRowRangeFilter, hashedDimRowFilter)
  }

  private def discreteDimensionFilters(query: InternalQuery, dimensionFilters: Map[Dimension, Set[IdType]]) = {
    val rangeScanDims = rangeScanDimensions(query, dimensionFilters)
    val discreteDimensions = query.table.dimensionSeq.toList.diff(rangeScanDims).filter(dimensionFilters.contains)

//    val discDimSize = if (discreteDimensions.nonEmpty) {
//      discreteDimensions.foldLeft(1L)((s, d) => s * dimensionFilters(d).size)
//    } else {
//      0
//    }

//    if (discDimSize > 0 && discDimSize < DISCRETE_TAG_FILTERS_LIMIT) {
//      CollectionUtils.crossJoin(discreteDimensions.map(d => dimensionFilters(d).toList))
//
//      throw new IllegalArgumentException("Fuzzy filters is not supported")
//      (Seq.empty[FuzzyRowFilter], Map.empty)
//    } else {
//      (Seq.empty, dimensionFilters.filterKeys(discreteDimensions.contains))
//    }
    (Seq.empty, dimensionFilters.filter(x => discreteDimensions.contains(x._1)))
  }

  private def createTimeFilter(fromTime: Long, toTime: Long, include: Set[Long], exclude: Set[Long]): Filtration.TimeFilter = {
    val baseFilter: Filtration.TimeFilter = t => t >= fromTime && t < toTime

    if (exclude.nonEmpty) {
      if (include.nonEmpty) {
        t => baseFilter(t) && include.contains(t) && !exclude.contains(t)
      } else {
        t => baseFilter(t) && !exclude.contains(t)
      }
    } else {
      if (include.nonEmpty) {
        t => baseFilter(t) && include.contains(t)
      } else {
        baseFilter
      }
    }
  }

  private def createRowFilter(table: Table, include: Map[Dimension, Set[IdType]], exclude: Map[Dimension, Set[IdType]]): Filtration.RowFilter = {
    if (exclude.nonEmpty) {
      if (include.nonEmpty) {
        rowFilter(table, (dimension, x) => include.get(dimension).forall(_.contains(x)) && !exclude.get(dimension).exists(_.contains(x)))
      } else {
        rowFilter(table, (dimension, x) => !exclude.get(dimension).exists(_.contains(x)))
      }
    } else {
      if (include.nonEmpty) {
        rowFilter(table, (tagName, x) => include.get(tagName).forall(_.contains(x)))
      } else {
        _ => true
      }
    }
  }

  private def rowFilter(table: Table, f: (Dimension, IdType) => Boolean): Filtration.RowFilter= {
    row =>
      row.key.tagsIds.zip(table.dimensionSeq).forall {
        case (Some(x), dim) => f(dim, x)
        case _ => true
      }
  }

  def createFilters(condition: Option[Condition]): Filters = {
    case class FilterParts(incValues: DimensionFilter[String],
                           incIds: DimensionFilter[IdType],
                           incTime: DimensionFilter[Time],
                           excValues: DimensionFilter[String],
                           excIds: DimensionFilter[IdType],
                           excTime: DimensionFilter[Time],
                           other: List[Condition])

    def createFilters(condition: Condition, filters: FilterParts): FilterParts = {
      condition match {
        case Equ(DimensionExpr(tag), ConstantExpr(c: String)) =>
          filters.copy(incValues = DimensionFilter[String](Map(tag -> Set(c))) and filters.incValues)

        case Equ(ConstantExpr(c: String), DimensionExpr(tag)) =>
          filters.copy(incValues = DimensionFilter[String](Map(tag -> Set(c))) and filters.incValues)

        case Equ(TimeExpr, ConstantExpr(c: Time)) =>
          filters.copy(incTime = DimensionFilter[Time](Map(TIME -> Set(c))) and filters.incTime)

        case Equ(ConstantExpr(c: Time), TimeExpr) =>
          filters.copy(incTime = DimensionFilter[Time](Map(TIME -> Set(c))) and filters.incTime)

        case In(DimensionExpr(tagName), consts) =>
          val valFilter = if (consts.nonEmpty) DimensionFilter(Map(tagName -> consts.asInstanceOf[Set[String]])) else NoResult[String]()
          filters.copy(incValues = valFilter and filters.incValues)

        case In(_: TimeExpr.type, consts) =>
          val valFilter = if (consts.nonEmpty) DimensionFilter(Map(TIME -> consts.asInstanceOf[Set[Time]])) else NoResult[Time]()
          filters.copy(incTime = valFilter and filters.incTime)

        case DimIdIn(DimensionExpr(tagName), tagIds) =>
          val idFilter = if (tagIds.nonEmpty) DimensionFilter(Map(tagName -> tagIds)) else NoResult[IdType]()
          filters.copy(incIds = idFilter and filters.incIds)

        case Neq(DimensionExpr(dim), ConstantExpr(c: String)) =>
          filters.copy(excValues = DimensionFilter[String](Map(dim -> Set(c))).or(filters.excValues))

        case Neq(ConstantExpr(c: String), DimensionExpr(tag)) =>
          filters.copy(excValues = DimensionFilter[String](Map(tag -> Set(c))).or(filters.excValues))

        case Neq(TimeExpr, ConstantExpr(c: Time)) =>
          filters.copy(excTime = DimensionFilter[Time](Map(TIME -> Set(c))).or(filters.excTime))

        case Neq(ConstantExpr(c: Time), TimeExpr) =>
          filters.copy(excTime = DimensionFilter[Time](Map(TIME -> Set(c))).or(filters.excTime))

        case NotIn(DimensionExpr(tagName), consts) =>
          val valFilter = if (consts.nonEmpty) DimensionFilter(Map(tagName -> consts.asInstanceOf[Set[String]])) else NoResult[String]()
          filters.copy(excValues = valFilter or filters.excValues)

        case NotIn(_: TimeExpr.type, consts) =>
          val valFilter = if (consts.nonEmpty) DimensionFilter(Map(TIME -> consts.asInstanceOf[Set[Time]])) else NoResult[Time]()
          filters.copy(excTime = valFilter or filters.excTime)

        case DimIdNotIn(DimensionExpr(tagName), dimIds) =>
          val idFilter = if (dimIds.nonEmpty) DimensionFilter(Map(tagName -> dimIds)) else NoResult[IdType]()
          filters.copy(excIds = idFilter or filters.excIds)

        case And(conditions) =>
          conditions.foldLeft(filters)((f, c) => createFilters(c, f))

        case In(t: TupleExpr[_, _], vs) =>
          val filters1 = createFilters(In(t.e1, vs.asInstanceOf[Set[(t.e1.Out, t.e2.Out)]].map(_._1)), filters)
          createFilters(In(t.e2, vs.asInstanceOf[Set[(t.e1.Out, t.e2.Out)]].map(_._2)), filters1)

        case Equ(TupleExpr(e1, e2), ConstantExpr(v: (_, _))) =>
          val filters1 = createFilters(In(e1.aux, Set(v._1.asInstanceOf[e1.Out])), filters)
          createFilters(In(e2.aux, Set(v._2.asInstanceOf[e2.Out])), filters1)

        case Equ(ConstantExpr(v: (_, _)), TupleExpr(e1, e2)) =>
          val filters1 = createFilters(In(e1.aux, Set(v._1.asInstanceOf[e1.Out])), filters)
          createFilters(In(e2.aux, Set(v._2.asInstanceOf[e2.Out])), filters1)

        case c => filters.copy(other = c :: filters.other)
      }
    }

    condition match {
      case Some(c) =>
        val filters = createFilters(c, FilterParts(EmptyFilter(), EmptyFilter(), EmptyFilter(), NoResult(), NoResult(), NoResult(), List.empty))

        val includeFilter = filters.incValues.map { case (tag, values) => tag -> valuesToIds(tag, values).values.toSet } and filters.incIds
        val excludeFilter = filters.excValues.map { case (tag, values) => tag -> valuesToIds(tag, values).values.toSet } or filters.excIds
        val cond = filters.other match {
          case Nil => None
          case x :: Nil => Some(x)
          case xs => Some(And(xs.reverse))
        }

        Filters(includeFilter, excludeFilter, filters.incTime, filters.excTime, cond)

      case None =>
        Filters(EmptyFilter(), EmptyFilter(), EmptyFilter(), EmptyFilter(), None)
    }
  }


  private def readRow(schemaContext: SchemaContext, bytes: Array[Byte], data: Array[Option[Any]], time: Long): Boolean = {
    val bb = ByteBuffer.wrap(bytes)
    util.Arrays.fill(data.asInstanceOf[Array[AnyRef]], None)
    var correct = true
    while (bb.hasRemaining && correct) {
      val tag = bb.get()
      schemaContext.fieldIndexMap.get(tag) match {
        case Some(field) =>
          data(tag) = Some(field.dataType.readable.read(bb))

        case None =>
          correct = false
          logger.warn(s"Unknown tag: $tag, in table: ${schemaContext.query.table.name}, row time: $time")
      }
    }
    correct
  }

  private def extractDataWithMetric(schemaContext: SchemaContext,
                                    valueDataBuilder: InternalRowBuilder,
                                    rows: Seq[TSDOutputRow[IdType]],
                                    timeFilter: TimeFilter,
                                    metricCollector: MetricQueryCollector
                           ): Seq[InternalRow] = {

    val indexedRows = rows.zipWithIndex

    val rowsByTags = rowsForTags(indexedRows, schemaContext)

    lazy val allTagValues = tagFields(rowsByTags, schemaContext)

    metricCollector.extractDataComputation.measure {
      val maxTag = schemaContext.query.table.metrics.map(_.tag).max

      val rowValues = Array.ofDim[Option[Any]](maxTag + 1)

      for {
        (row, idx) <- indexedRows
        tagValues = allTagValues.row(idx)
        (offset, bytes) <- row.values.toSeq
        time = row.key.baseTime + offset if timeFilter(time) && readRow(schemaContext, bytes, rowValues, time)
      } yield {

        schemaContext.query.exprs.foreach {
          case e@DimensionExpr(dim) => valueDataBuilder.set(e, tagValues.get(dim))
          case e@MetricExpr(field) => valueDataBuilder.set(e, rowValues(field.tag))
          case TimeExpr => valueDataBuilder.set(TimeExpr, Some(Time(time)))
          case e => throw new IllegalArgumentException(s"Unsupported expression $e passed to DAO")
        }

        valueDataBuilder.buildAndReset()
      }
    }
  }

  override def isSupportedCondition(condition: Condition): Boolean = {
    condition match {
      case SimpleCondition(BinaryOperationExpr(_, _: TimeExpr.type, ConstantExpr(_))) => true
      case SimpleCondition(BinaryOperationExpr(_, ConstantExpr(_), _: TimeExpr.type)) => true
      case _: DimIdIn => true
      case _: DimIdNotIn => true
      case Equ(_: DimensionExpr, ConstantExpr(_)) => true
      case Equ(ConstantExpr(_), _: DimensionExpr) => true
      case Neq(_: DimensionExpr, ConstantExpr(_)) => true
      case Neq(ConstantExpr(_), _: DimensionExpr) => true
      case In(_: DimensionExpr, _) => true
      case NotIn(_: DimensionExpr, _) => true
      case _ => false
    }
  }

  private def rowRanges(baseTimeList: NumericRange[IdType], prefixes: Seq[Set[IdType]], ranges: Set[(IdType, IdType)]) = {
    val ps = if (prefixes.isEmpty) List(List()) else CollectionUtils.crossJoin(prefixes.map(_.toList).toList)

    if (ranges.isEmpty) {
      baseTimeList.map { baseTime =>
        rowRange(baseTime, Nil, None)
      }
    } else {
      for {
        baseTime <- baseTimeList
        prefix <- ps
        (start, stop) <- ranges
      } yield {
        rowRange(baseTime, prefix, Some((start, stop)))
      }
    }
  }

  private def rowRange(baseTime: IdType, prefix: List[IdType], range: Option[(Long, Long)]) = {
    val (timeInc, sizeInc) = if (range.isDefined) (0, 1) else (1, 0)

    val bufSize = (prefix.size + 1 + sizeInc) * Bytes.SIZEOF_LONG

    val startBuffer = ByteBuffer.allocate(bufSize)
    val stopBuffer = ByteBuffer.allocate(bufSize)
    startBuffer.put(Bytes.toBytes(baseTime))
    stopBuffer.put(Bytes.toBytes(baseTime + timeInc))
    prefix.foreach { v =>
      startBuffer.put(Bytes.toBytes(v))
      stopBuffer.put(Bytes.toBytes(v))
    }

    range.foreach { case (start, stop) =>
      startBuffer.put(Bytes.toBytes(start))
      stopBuffer.put(Bytes.toBytes(stop + 1))
    }
    new RowRange(startBuffer.array(), true, stopBuffer.array(), false)
  }

  def hashedDimensionRanges(hashedDimension: Dimension, rangeScanDimensions: Seq[Dimension], dimensionFilters: Map[Dimension, Set[IdType]]): Set[(IdType, IdType)] = {
    val nonHashedSize = rangeScanDimensions.foldLeft(0)((s, d) => dimensionFilters(d).size)
    val availSize = QUERY_TAG_LIMIT - nonHashedSize
    val ids = dimensionFilters(hashedDimension)

    hashedDimensionRanges(ids, availSize)
  }

  def hashedDimensionRanges(ids: Set[IdType], availSize: Int): Set[(IdType, IdType)] = {
    val rs = if (ids.isEmpty) {
      Set.empty
    } else {
      val xs = ids.map(_ >>> 32).to[ArrayBuffer]

      val clusters = KMeans.perform(xs, availSize)

      clusters.values.flatMap { xs =>
        if (xs.nonEmpty) {
          Some(xs.min -> xs.max)
        } else {
          None
        }
      }.toSet
    }

    val rangesIds = rs.map { r =>
      ids.filter { id =>
        val hid = id >>> 32
        hid >= r._1 && hid <= r._2
      }
    }

    rangesIds.flatMap { is =>
      if (is.nonEmpty) {
        Some(is.min -> is.max)
      } else {
        None
      }
    }
  }

  def rangeScanDimensions(query: InternalQuery, dimensionFilters: Map[Dimension, Set[IdType]]): Seq[Dimension] = {

    val continuousDims = query.table.dimensionSeq.takeWhile(dimensionFilters.contains)

    val sizes = continuousDims.scanLeft(1L)((s, d) => s * dimensionFilters(d).size).drop(1)

    val sizeLimitedDims = continuousDims.zip(sizes).filter(_._2 < QUERY_TAG_LIMIT).map(_._1)

    val hashedDimIdx = continuousDims.indexWhere(d => d.hashFunction.isDefined)

    if (hashedDimIdx >= 0) {
      if (hashedDimIdx < sizeLimitedDims.size) {
        sizeLimitedDims.take(hashedDimIdx + 1)
      } else if (hashedDimIdx == sizeLimitedDims.size) {
        sizeLimitedDims :+ query.table.dimensionSeq.apply(hashedDimIdx)
      } else {
        sizeLimitedDims
      }
    } else {
      sizeLimitedDims
    }
  }

  private def rowsForTags(indexedRows: Seq[(TSDOutputRow[IdType], Int)], schemaContext: SchemaContext): SparseTable[Dimension, IdType, Seq[Int]] = {
    val tagRowMap = schemaContext.requiredTags.map { dim =>
      val tagIndex = schemaContext.tagIndexMap(dim)
      dim -> indexedRows
        .flatMap { case (row, index) => row.key.tagsIds(tagIndex).map(index -> _) }
        .groupBy(_._2)
        .mapValues(_.map(_._1))
    }.toMap

    SparseTable(tagRowMap)
  }

  private def tagFields(tagTable: SparseTable[Dimension, IdType, Seq[Int]], schemaContext: SchemaContext): SparseTable[Int, Dimension, String] = {
    val allValues = schemaContext.requiredTags.map { dim =>
      val tagIdRows = tagTable.row(dim)
      val tagValues = idsToValues(dim, tagIdRows.keySet) //dictionary(tagName).values(tagIdRows.keySet)
      val data = tagValues.flatMap { case (tagId, tagValue) =>
        tagIdRows.get(tagId).toSeq.flatMap(_.map(row => (row, dim, tagValue)))
      }
      SparseTable(data)
    }

    allValues.foldLeft(SparseTable.empty[Int, Dimension, String])(_ ++ _)
  }

  private def familiesQueried(query: InternalQuery): Set[Int] = {
    val groups = query.exprs.flatMap(_.requiredMetrics.map(_.group))

    if (groups.nonEmpty) {
      groups
    } else {
      Set(Metric.defaultGroup)
    }
  }

  override def idsToValues(dimension: Dimension, ids: Set[IdType]): Map[IdType, String] = {
    dictionaryProvider.dictionary(dimension).values(ids)
  }

  override def valuesToIds(dimension: Dimension, values: Set[String]): Map[String, IdType] = {
    dictionaryProvider.dictionary(dimension).findIdsByValues(values)
  }
}
