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
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.hbase.client.{ Result => HResult }
import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.schema._
import org.yupana.api.types.ByteReaderWriter
import org.yupana.api.utils.ConditionMatchers._
import org.yupana.api.utils.{ PrefetchedSortedSetIterator, SortedSetIterator }
import org.yupana.core.QueryContext
import org.yupana.core.dao._
import org.yupana.core.model.{ BatchDataset, DatasetSchema, InternalQuery }
import org.yupana.core.utils.{ ConditionUtils, FlatAndCondition }
import org.yupana.core.utils.metric.MetricQueryCollector
import org.yupana.serialization.{ MemoryBuffer, MemoryBufferEvalReaderWriter }

import scala.util.Try

object TSDaoHBaseBase {
  val CROSS_JOIN_LIMIT = 500000
  val RANGE_FILTERS_LIMIT = 100000
  val EXTRACT_BATCH_SIZE = 100000
  val PUTS_BATCH_SIZE = 1000
}

trait TSDaoHBaseBase[Collection[_]] extends TSDao[Collection, Long] with StrictLogging {

  import TSDaoHBaseBase._

  implicit val readerWriter: ByteReaderWriter[MemoryBuffer] = MemoryBufferEvalReaderWriter

  type IdType = Long
  type TimeFilter = Long => Boolean
  type RowFilter = TSDRowKey => Boolean

  val schema: Schema

  def dictionaryProvider: DictionaryProvider

  def executeScans(
      queryContext: InternalQueryContext,
      intervals: Seq[(Long, Long)],
      rangeScanDims: Iterator[Map[Dimension, Seq[_]]]
  ): Collection[HResult]

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
            val s = HBaseUtils.baseTime(t, table)
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

  override def query(
      query: InternalQuery,
      queryContext: QueryContext,
      datasetSchema: DatasetSchema,
      metricCollector: MetricQueryCollector
  ): Collection[BatchDataset] = {

    val context = InternalQueryContext(query, metricCollector)
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
      mr.empty[BatchDataset]
    } else {
      val rangeScanDimIterators = sizeLimitedRangeScanDims.map { d =>
        d -> prefetchedDimIterators(d)
      }.toMap

      val rowPostFilter: RowFilter = if (flatAndConditions.distinct.size == 1) {
        val includeRowFilter = prefetchedDimIterators.filter { case (d, _) => !sizeLimitedRangeScanDims.contains(d) }

        val excludeRowFilter = squashedFilters.allExcludes.filter {
          case (d, _) => !sizeLimitedRangeScanDims.contains(d)
        }

        createRowFilter(query.table, includeRowFilter, excludeRowFilter)

      } else { _ => true }

      val timeFilter = createTimeFilter(
        intervals,
        squashedFilters.includeTime.getOrElse(Set.empty),
        squashedFilters.excludeTime.getOrElse(Set.empty)
      )

      val rangeScanDimIds = rangeScanFilters(rangeScanDimIterators)

      val rows = executeScans(context, intervals, rangeScanDimIds)

      import org.yupana.core.utils.metric.MetricUtils._

      val table = query.table // prevent query serialization in Spark

      mr.batchFlatMap(rows, EXTRACT_BATCH_SIZE) { rs =>
        val filtered = context.metricsCollector.filterRows.measure(rs.size) {
          rs.filter(r => rowPostFilter(HBaseUtils.parseRowKey(r.getRow, table)))
        }

        val batchIterator = new TSDHBaseRowIterator(context, filtered.iterator, datasetSchema)

        batchIterator.map { batch =>
          batch.foreach { rowNum =>
            val time = batch.getTime(rowNum)
            if (!timeFilter(time.millis)) {
              batch.setDeleted(rowNum)
            }
          }
          batch
        }
      }.withSavedMetrics(context.metricsCollector)
    }
  }

  private def valuesToIds(
      dimension: DictionaryDimension,
      values: SortedSetIterator[String]
  ): SortedSetIterator[IdType] = {
    val dictionary = dictionaryProvider.dictionary(dimension)
    val it = dictionary.findIdsByValues(values.toSet).values.toSeq.sortWith(dimension.rOrdering.lt).iterator
    SortedSetIterator(it)
  }

  private def rangeScanDimensions(
      query: InternalQuery,
      prefetchedDimIterators: Map[Dimension, PrefetchedSortedSetIterator[_]]
  ) = {

    val continuousDims = query.table.dimensionSeq.takeWhile(prefetchedDimIterators.contains)
    val sizes = continuousDims
      .scanLeft(1L) {
        case (size, dim) =>
          val it = prefetchedDimIterators(dim)
          val itSize = if (it.isAllFetched) it.fetched.length else RANGE_FILTERS_LIMIT
          size * itSize
      }
      .drop(1)

    val sizeLimitedRangeScanDims = continuousDims.zip(sizes).takeWhile(_._2 <= CROSS_JOIN_LIMIT).map(_._1)
    sizeLimitedRangeScanDims
  }

  private def rangeScanFilters(
      dimensionIds: Map[Dimension, PrefetchedSortedSetIterator[_]]
  ): Iterator[Map[Dimension, Seq[_]]] = {

    val dimSeq = dimensionIds.keys

    val (completelyFetchedDimIts, partiallyFetchedDimIts) = dimensionIds.partition(_._2.isAllFetched)

    if (partiallyFetchedDimIts.size > 1) {
      throw new IllegalStateException(
        s"More then one dimension in query have size greater " +
          s"than $RANGE_FILTERS_LIMIT [${partiallyFetchedDimIts.keys.mkString(", ")}]"
      )
    }

    val fetchedDimIds = completelyFetchedDimIts.map { case (dim, ids) => dim -> ids.fetched.toSeq }

    partiallyFetchedDimIts.headOption match {
      case Some((_, pids)) =>
        pids.grouped(RANGE_FILTERS_LIMIT).map { batch =>
          dimSeq.map(d => d -> fetchedDimIds.getOrElse(d, batch)).toMap
        }

      case None =>
        Iterator(fetchedDimIds)
    }
  }

  private def createTimeFilter(
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

  private def createRowFilter(
      table: Table,
      include: Map[Dimension, SortedSetIterator[_]],
      exclude: Map[Dimension, SortedSetIterator[_]]
  ): RowFilter = {

    val includeMap: Map[Dimension, Set[Any]] = include.map { case (k, v) => k -> v.toSet }
    val excludeMap: Map[Dimension, Set[Any]] = exclude.map { case (k, v) => k -> v.toSet }

    if (excludeMap.nonEmpty) {
      if (includeMap.nonEmpty) {
        rowFilter(
          table,
          (dim, x) => includeMap.get(dim).forall(_.contains(x)) && !excludeMap.get(dim).exists(_.contains(x))
        )
      } else {
        rowFilter(table, (dim, x) => !excludeMap.get(dim).exists(_.contains(x)))
      }
    } else {
      if (includeMap.nonEmpty) {
        rowFilter(table, (dim, x) => includeMap.get(dim).forall(_.contains(x)))
      } else { _ =>
        true
      }
    }
  }

  private def rowFilter(table: Table, f: (Dimension, Any) => Boolean): RowFilter = { rowKey =>
    rowKey.dimReprs.zip(table.dimensionSeq).forall {
      case (Some(x), dim) => f(dim, x)
      case _              => true
    }
  }

  private def dimIdValueFromString[R](dim: Dimension.AuxR[R], value: String): Option[R] = {
    Try(Hex.decodeHex(value.toCharArray)).toOption.map { a =>
      dim.rStorable.read(MemoryBuffer.ofBytes(a))(readerWriter)
    }
  }

  def createFilters(condition: Option[Condition]): Filters = {
    def handleEq(condition: Condition, builder: Filters.Builder): Filters.Builder = {
      condition match {
        case EqExpr(DimensionExpr(dim), ConstantExpr(c)) =>
          builder.includeValue(dim, c)

        case EqExpr(ConstantExpr(c), DimensionExpr(dim)) =>
          builder.includeValue(dim, c)

        case EqString(LowerExpr(DimensionExpr(dim)), ConstantExpr(c)) =>
          builder.includeValue(dim.aux, c)

        case EqString(ConstantExpr(c), LowerExpr(DimensionExpr(dim))) =>
          builder.includeValue(dim.aux, c)

        case EqString(DimensionIdExpr(dim), ConstantExpr(c)) =>
          builder.includeIds(dim.aux, dimIdValueFromString(dim.aux, c).toSeq)

        case EqString(ConstantExpr(c), DimensionIdExpr(dim)) =>
          builder.includeIds(dim.aux, dimIdValueFromString(dim.aux, c).toSeq)

        case EqTime(TimeExpr, ConstantExpr(c)) =>
          builder.includeTime(c)

        case EqTime(ConstantExpr(c), TimeExpr) =>
          builder.includeTime(c)

        case EqUntyped(t: TupleExpr[a, b], v: ValueExpr[_]) =>
          val (x, y) = ConditionUtils.value(v.asInstanceOf[ValueExpr[(a, b)]])
          val filters1 = createFilters(InExpr(t.e1, Set[ValueExpr[a]](ConstantExpr(x)(t.e1.dataType))), builder)
          createFilters(InExpr(t.e2, Set[ValueExpr[b]](ConstantExpr(y)(t.e2.dataType))), filters1)

        case EqUntyped(v: TupleValueExpr[_, _], t: TupleExpr[a, b]) =>
          val (x, y) = ConditionUtils.value(v.asInstanceOf[ValueExpr[(a, b)]])
          val filters1 = createFilters(InExpr(t.e1, Set[ValueExpr[a]](ConstantExpr(x)(t.e1.dataType))), builder)
          createFilters(InExpr(t.e2, Set[ValueExpr[b]](ConstantExpr(y)(t.e2.dataType))), filters1)

        case _ => builder
      }
    }

    def handleNeq(condition: Condition, builder: Filters.Builder): Filters.Builder = {
      condition match {
        case NeqExpr(DimensionExpr(dim), ConstantExpr(c)) =>
          builder.excludeValue(dim, c)

        case NeqExpr(ConstantExpr(c), DimensionExpr(dim)) =>
          builder.excludeValue(dim, c)

        case NeqString(LowerExpr(DimensionExpr(dim)), ConstantExpr(c)) =>
          builder.excludeValue(dim.aux, c)

        case NeqString(ConstantExpr(c), LowerExpr(DimensionExpr(dim))) =>
          builder.excludeValue(dim.aux, c)

        case NeqString(DimensionIdExpr(dim), ConstantExpr(c)) =>
          builder.excludeIds(dim.aux, dimIdValueFromString(dim.aux, c).toSeq)

        case NeqString(ConstantExpr(c), DimensionIdExpr(dim)) =>
          builder.excludeIds(dim.aux, dimIdValueFromString(dim.aux, c).toSeq)

        case NeqTime(TimeExpr, ConstantExpr(c)) =>
          builder.excludeTime(c)

        case NeqTime(ConstantExpr(c), TimeExpr) =>
          builder.excludeTime(c)

        case _ => builder
      }
    }

    def handleIn(condition: Condition, builder: Filters.Builder): Filters.Builder = {
      condition match {
        case InExpr(DimensionExpr(dim), consts) =>
          builder.includeValues(dim, consts.map(ConditionUtils.value))

        case InString(LowerExpr(DimensionExpr(dim)), consts) =>
          builder.includeValues(dim, consts.map(ConditionUtils.value))

        case InTime(TimeExpr, consts) =>
          builder.includeTime(consts.map(ConditionUtils.value))

        case InString(DimensionIdExpr(dim), dimIds) =>
          builder.includeIds(
            dim.aux,
            dimIds.toSeq.flatMap(v => dimIdValueFromString(dim.aux, ConditionUtils.value(v)))
          )

        case InUntyped(t: TupleExpr[a, b], vs) =>
          val values: Set[(a, b)] = vs.asInstanceOf[Set[ValueExpr[(a, b)]]].map(ConditionUtils.value)
          val filters1 = createFilters(
            InExpr(t.e1, values.map(x => ConstantExpr(x._1)(t.e1.dataType)).asInstanceOf[Set[ValueExpr[a]]]),
            builder
          )
          createFilters(
            InExpr(t.e2, values.map(x => ConstantExpr(x._2)(t.e2.dataType)).asInstanceOf[Set[ValueExpr[b]]]),
            filters1
          )

        case _ => builder
      }
    }

    def handleNotIn(condition: Condition, builder: Filters.Builder): Filters.Builder = {
      condition match {
        case NotInExpr(DimensionExpr(dim), consts) =>
          builder.excludeValues(dim, consts.map(ConditionUtils.value))

        case NotInString(LowerExpr(DimensionExpr(dim)), consts) =>
          builder.excludeValues(dim, consts.map(ConditionUtils.value))

        case NotInString(DimensionIdExpr(dim), dimIds) =>
          builder.excludeIds(
            dim.aux,
            dimIds.toSeq.flatMap(v => dimIdValueFromString(dim.aux, ConditionUtils.value(v)))
          )

        case NotInTime(TimeExpr, consts) =>
          builder.excludeTime(consts.map(ConditionUtils.value))

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

  override def isSupportedCondition(condition: Condition): Boolean = {
    def handleEq(condition: EqExpr[_]): Boolean = {
      condition match {
        case EqTime(TimeExpr, ConstantExpr(_))                         => true
        case EqTime(ConstantExpr(_), TimeExpr)                         => true
        case EqExpr(_: DimensionExpr[_], ConstantExpr(_))              => true
        case EqExpr(ConstantExpr(_), _: DimensionExpr[_])              => true
        case EqString(LowerExpr(_: DimensionExpr[_]), ConstantExpr(_)) => true
        case EqString(ConstantExpr(_), LowerExpr(_: DimensionExpr[_])) => true
        case EqString(_: DimensionIdExpr, ConstantExpr(_))             => true
        case EqString(ConstantExpr(_), _: DimensionIdExpr)             => true
        case _                                                         => false
      }
    }

    def handleNeq(condition: NeqExpr[_]): Boolean = {
      condition match {
        case NeqTime(TimeExpr, ConstantExpr(_))                         => true
        case NeqTime(ConstantExpr(_), TimeExpr)                         => true
        case NeqExpr(_: DimensionExpr[_], ConstantExpr(_))              => true
        case NeqExpr(ConstantExpr(_), _: DimensionExpr[_])              => true
        case NeqString(LowerExpr(_: DimensionExpr[_]), ConstantExpr(_)) => true
        case NeqString(LowerExpr(ConstantExpr(_)), _: DimensionExpr[_]) => true
        case NeqString(_: DimensionIdExpr, ConstantExpr(_))             => true
        case NeqString(ConstantExpr(_), _: DimensionIdExpr)             => true
        case _                                                          => false
      }
    }

    condition match {
      case e: EqExpr[_]                                   => handleEq(e)
      case e: NeqExpr[_]                                  => handleNeq(e)
      case GtTime(TimeExpr, ConstantExpr(_))              => true
      case GtTime(ConstantExpr(_), TimeExpr)              => true
      case LtTime(TimeExpr, ConstantExpr(_))              => true
      case LtTime(ConstantExpr(_), TimeExpr)              => true
      case GeTime(TimeExpr, ConstantExpr(_))              => true
      case GeTime(ConstantExpr(_), TimeExpr)              => true
      case LeTime(TimeExpr, ConstantExpr(_))              => true
      case LeTime(ConstantExpr(_), TimeExpr)              => true
      case InTime(TimeExpr, _)                            => true
      case NotInTime(TimeExpr, _)                         => true
      case _: DimIdInExpr[_]                              => true
      case _: DimIdNotInExpr[_]                           => true
      case InExpr(_: DimensionExpr[_], _)                 => true
      case NotInExpr(_: DimensionExpr[_], _)              => true
      case InString(LowerExpr(_: DimensionExpr[_]), _)    => true
      case NotInString(LowerExpr(_: DimensionExpr[_]), _) => true
      case InString(_: DimensionIdExpr, _)                => true
      case NotInString(_: DimensionIdExpr, _)             => true
      case _                                              => false
    }
  }
}
