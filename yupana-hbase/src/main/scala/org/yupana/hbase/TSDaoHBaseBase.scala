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
import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.schema.{ Dimension, Table }
import org.yupana.api.utils.{ DimOrdering, PrefetchedSortedSetIterator, SortedSetIterator }
import org.yupana.core.MapReducible
import org.yupana.core.dao._
import org.yupana.core.model.{ InternalQuery, InternalRow, InternalRowBuilder }
import org.yupana.core.utils.metric.MetricQueryCollector
import org.yupana.core.utils.{ SparseTable, TimeBoundedCondition }
import org.yupana.hbase.Filtration.TimeFilter

import scala.language.higherKinds

trait TSDaoHBaseBase[Collection[_]] extends TSReadingDao[Collection, Long] with StrictLogging {
  type IdType = Long

  import org.yupana.core.utils.ConditionMatchers.{ Equ, Neq }

  val TIME = Dimension("time")

  val CROSS_JOIN_LIMIT = 500000
  val RANGE_FILTERS_LIMIT = 100000
  val FUZZY_FILTERS_LIMIT = 20
  val EXTRACT_BATCH_SIZE = 10000

  def mapReduceEngine(metricQueryCollector: MetricQueryCollector): MapReducible[Collection]
  def dictionaryProvider: DictionaryProvider

  case class Filters(
      includeDims: DimensionFilter[IdType],
      excludeDims: DimensionFilter[IdType],
      includeTime: DimensionFilter[Time],
      excludeTime: DimensionFilter[Time],
      condition: Option[Condition]
  )

  def executeScans(
      queryContext: InternalQueryContext,
      from: Long,
      to: Long,
      rangeScanDims: Iterator[Map[Dimension, Seq[IdType]]]
  ): Collection[TSDOutputRow[IdType]]

  override def query(
      query: InternalQuery,
      valueDataBuilder: InternalRowBuilder,
      metricCollector: MetricQueryCollector
  ): Collection[InternalRow] = {

    val tbc = TimeBoundedCondition(query.condition)

    if (tbc.size != 1) throw new IllegalArgumentException("Only one condition is supported")

    val condition = tbc.head

    val from = condition.from.getOrElse(throw new IllegalArgumentException("FROM time is not defined"))
    val to = condition.to.getOrElse(throw new IllegalArgumentException("TO time is not defined"))

    val filters = metricCollector.createDimensionFilters.measure(1) {
      val c = if (condition.conditions.nonEmpty) Some(AndExpr(condition.conditions)) else None
      createFilters(c)
    }

    val dimFilter = filters.includeDims exclude filters.excludeDims

    val prefetchedDimIterators = dimFilter.toMap.map { case (d, it) => d -> it.prefetch(RANGE_FILTERS_LIMIT) }

    val sizeLimitedRangeScanDims = rangeScanDimensions(query, prefetchedDimIterators)

    val rangeScanDimIds = dimFilter match {
      case NoResult() => Iterator.empty
      case _ =>
        val rangeScanDimIterators = sizeLimitedRangeScanDims.map(d => d -> prefetchedDimIterators(d)).toMap
        rangeScanFilters(query, from, to, rangeScanDimIterators)
    }

    val context = InternalQueryContext(query, metricCollector)

    val rows = executeScans(context, from, to, rangeScanDimIds)

    val includeRowFilter = DimensionFilter(
      prefetchedDimIterators.filterKeys(d => !sizeLimitedRangeScanDims.contains(d))
    )
    val rowFilter = createRowFilter(query.table, includeRowFilter, filters.excludeDims)
    val timeFilter = createTimeFilter(from, to, filters.includeTime, filters.excludeTime)

    val mr = mapReduceEngine(metricCollector)

    mr.batchFlatMap(rows, EXTRACT_BATCH_SIZE) { rs =>
      val filtered = context.metricsCollector.filterRows.measure(rs.size) {
        rs.filter(rowFilter)
      }
      extractData(context, valueDataBuilder, filtered, timeFilter).iterator
    }
  }

  override def idsToValues(
      dimension: Dimension,
      ids: Set[IdType],
      metricCollector: MetricQueryCollector
  ): Map[IdType, String] = {
    dictionaryProvider.dictionary(dimension).values(ids, metricCollector)
  }

  override def valuesToIds(dimension: Dimension, values: SortedSetIterator[String]): SortedSetIterator[IdType] = {
    val dictionary = dictionaryProvider.dictionary(dimension)
    val ord = implicitly[DimOrdering[IdType]]
    val it = dictionary.findIdsByValues(values.toSet).values.toSeq.sortWith(ord.lt).iterator
    SortedSetIterator(it)
  }

  private def rangeScanDimensions(
      query: InternalQuery,
      prefetchedDimIterators: Map[Dimension, PrefetchedSortedSetIterator[IdType]]
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
      query: InternalQuery,
      from: IdType,
      to: IdType,
      dimensionIds: Map[Dimension, PrefetchedSortedSetIterator[IdType]]
  ): Iterator[Map[Dimension, Seq[IdType]]] = {

    val (completelyFetchedDimIts, partiallyFetchedDimIts) = dimensionIds.partition(_._2.isAllFetched)

    if (partiallyFetchedDimIts.size > 1) {
      throw new IllegalStateException(
        s"More then one dimension in query have size greater " +
          s"than $RANGE_FILTERS_LIMIT [${partiallyFetchedDimIts.keys.mkString(", ")}]"
      )
    }

    val fetchedDimIds = completelyFetchedDimIts.map { case (dim, ids) => dim -> ids.fetched.toSeq }

    partiallyFetchedDimIts.headOption match {
      case Some((pd, pids)) =>
        pids.grouped(RANGE_FILTERS_LIMIT).map { batch =>
          fetchedDimIds + (pd -> batch)
        }

      case None =>
        Iterator(fetchedDimIds)
    }
  }

  private def createTimeFilter(
      fromTime: Long,
      toTime: Long,
      include: DimensionFilter[Time],
      exclude: DimensionFilter[Time]
  ): Filtration.TimeFilter = {
    val baseFilter: Filtration.TimeFilter = t => t >= fromTime && t < toTime

    val includeSet = include.toMap.getOrElse(TIME, Set.empty).map(_.millis).toSet
    val excludeSet = exclude.toMap.getOrElse(TIME, Set.empty).map(_.millis).toSet

    if (excludeSet.nonEmpty) {
      if (includeSet.nonEmpty) { t =>
        baseFilter(t) && includeSet.contains(t) && !excludeSet.contains(t)
      } else { t =>
        baseFilter(t) && !excludeSet.contains(t)
      }
    } else {
      if (includeSet.nonEmpty) { t =>
        baseFilter(t) && includeSet.contains(t)
      } else {
        baseFilter
      }
    }
  }

  private def createRowFilter(
      table: Table,
      include: DimensionFilter[IdType],
      exclude: DimensionFilter[IdType]
  ): Filtration.RowFilter = {

    val includeMap = include.toMap.map { case (k, v) => k -> v.toSet }
    val excludeMap = exclude.toMap.map { case (k, v) => k -> v.toSet }

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

  private def rowFilter(table: Table, f: (Dimension, IdType) => Boolean): Filtration.RowFilter = { row =>
    row.key.dimIds.zip(table.dimensionSeq).forall {
      case (Some(x), dim) => f(dim, x)
      case _              => true
    }
  }

  def createFilters(condition: Option[Condition]): Filters = {
    case class FilterParts(
        incValues: DimensionFilter[String],
        incIds: DimensionFilter[IdType],
        incTime: DimensionFilter[Time],
        excValues: DimensionFilter[String],
        excIds: DimensionFilter[IdType],
        excTime: DimensionFilter[Time],
        other: List[Condition]
    )

    def createFilters(condition: Condition, filters: FilterParts): FilterParts = {
      condition match {
        case Equ(DimensionExpr(dim), ConstantExpr(c: String)) =>
          filters.copy(incValues = DimensionFilter[String](dim, c) and filters.incValues)

        case Equ(ConstantExpr(c: String), DimensionExpr(dim)) =>
          filters.copy(incValues = DimensionFilter[String](dim, c) and filters.incValues)

        case Equ(TimeExpr, ConstantExpr(c: Time)) =>
          filters.copy(incTime = DimensionFilter[Time](TIME, c) and filters.incTime)

        case Equ(ConstantExpr(c: Time), TimeExpr) =>
          filters.copy(incTime = DimensionFilter[Time](TIME, c) and filters.incTime)

        case InExpr(DimensionExpr(dim), consts) =>
          val valFilter =
            if (consts.nonEmpty) DimensionFilter(dim, consts.asInstanceOf[Set[String]]) else NoResult[String]()
          filters.copy(incValues = valFilter and filters.incValues)

        case InExpr(_: TimeExpr.type, consts) =>
          val valFilter =
            if (consts.nonEmpty) DimensionFilter(TIME, consts.asInstanceOf[Set[Time]]) else NoResult[Time]()
          filters.copy(incTime = valFilter and filters.incTime)

        case DimIdInExpr(DimensionExpr(dim), dimIds) =>
          val idFilter = if (dimIds.nonEmpty) DimensionFilter(Map(dim -> dimIds)) else NoResult[IdType]()
          filters.copy(incIds = idFilter and filters.incIds)

        case Neq(DimensionExpr(dim), ConstantExpr(c: String)) =>
          filters.copy(excValues = DimensionFilter[String](dim, c).or(filters.excValues))

        case Neq(ConstantExpr(c: String), DimensionExpr(dim)) =>
          filters.copy(excValues = DimensionFilter[String](dim, c).or(filters.excValues))

        case Neq(TimeExpr, ConstantExpr(c: Time)) =>
          filters.copy(excTime = DimensionFilter[Time](TIME, c).or(filters.excTime))

        case Neq(ConstantExpr(c: Time), TimeExpr) =>
          filters.copy(excTime = DimensionFilter[Time](TIME, c).or(filters.excTime))

        case NotInExpr(DimensionExpr(dim), consts) =>
          val valFilter =
            if (consts.nonEmpty) DimensionFilter(dim, consts.asInstanceOf[Set[String]]) else NoResult[String]()
          filters.copy(excValues = valFilter or filters.excValues)

        case NotInExpr(_: TimeExpr.type, consts) =>
          val valFilter =
            if (consts.nonEmpty) DimensionFilter(TIME, consts.asInstanceOf[Set[Time]]) else NoResult[Time]()
          filters.copy(excTime = valFilter or filters.excTime)

        case DimIdNotInExpr(DimensionExpr(dim), dimIds) =>
          val idFilter = if (dimIds.nonEmpty) DimensionFilter(dim, dimIds) else NoResult[IdType]()
          filters.copy(excIds = idFilter or filters.excIds)

        case AndExpr(conditions) =>
          conditions.foldLeft(filters)((f, c) => createFilters(c, f))

        case InExpr(t: TupleExpr[_, _], vs) =>
          val filters1 = createFilters(InExpr(t.e1, vs.asInstanceOf[Set[(t.e1.Out, t.e2.Out)]].map(_._1)), filters)
          createFilters(InExpr(t.e2, vs.asInstanceOf[Set[(t.e1.Out, t.e2.Out)]].map(_._2)), filters1)

        case Equ(TupleExpr(e1, e2), ConstantExpr(v: (_, _))) =>
          val filters1 = createFilters(InExpr(e1.aux, Set(v._1.asInstanceOf[e1.Out])), filters)
          createFilters(InExpr(e2.aux, Set(v._2.asInstanceOf[e2.Out])), filters1)

        case Equ(ConstantExpr(v: (_, _)), TupleExpr(e1, e2)) =>
          val filters1 = createFilters(InExpr(e1.aux, Set(v._1.asInstanceOf[e1.Out])), filters)
          createFilters(InExpr(e2.aux, Set(v._2.asInstanceOf[e2.Out])), filters1)

        case c => filters.copy(other = c :: filters.other)
      }
    }

    condition match {
      case Some(c) =>
        val filters = createFilters(
          c,
          FilterParts(EmptyFilter(), EmptyFilter(), EmptyFilter(), NoResult(), NoResult(), NoResult(), List.empty)
        )

        val includeFilter = filters.incValues.map { case (dim, values) => dim -> valuesToIds(dim, values) } and filters.incIds
        val excludeFilter = filters.excValues.map { case (dim, values) => dim -> valuesToIds(dim, values) } or filters.excIds
        val cond = filters.other match {
          case Nil      => None
          case x :: Nil => Some(x)
          case xs       => Some(AndExpr(xs.reverse))
        }

        Filters(includeFilter, excludeFilter, filters.incTime, filters.excTime, cond)

      case None =>
        Filters(EmptyFilter(), EmptyFilter(), EmptyFilter(), EmptyFilter(), None)
    }
  }

  private def readRow(
      context: InternalQueryContext,
      bytes: Array[Byte],
      data: Array[Option[Any]],
      time: Long
  ): Boolean = {
    val bb = ByteBuffer.wrap(bytes)
    util.Arrays.fill(data.asInstanceOf[Array[AnyRef]], None)
    var correct = true
    while (bb.hasRemaining && correct) {
      val tag = bb.get()
      context.fieldIndexMap.get(tag) match {
        case Some(field) =>
          data(tag) = Some(field.dataType.readable.read(bb))

        case None =>
          correct = false
          logger.warn(s"Unknown tag: $tag, in table: ${context.table.name}, row time: $time")
      }
    }
    correct
  }

  private def extractData(
      context: InternalQueryContext,
      valueDataBuilder: InternalRowBuilder,
      rows: Seq[TSDOutputRow[IdType]],
      timeFilter: TimeFilter
  ): Seq[InternalRow] = {

    val indexedRows = rows.zipWithIndex

    lazy val allTagValues = context.metricsCollector.dimensionValuesForIds.measure(rows.size) {
      val rowsByTags = rowsForDims(indexedRows, context)
      dimFields(rowsByTags, context)
    }

    context.metricsCollector.extractDataComputation.measure(rows.size) {
      val maxTag = context.table.metrics.map(_.tag).max

      val rowValues = Array.ofDim[Option[Any]](maxTag + 1)

      for {
        (row, idx) <- indexedRows
        tagValues = allTagValues.row(idx)
        (offset, bytes) <- row.values.toSeq
        time = row.key.baseTime + offset if timeFilter(time) && readRow(context, bytes, rowValues, time)
      } yield {

        context.exprs.foreach {
          case e @ DimensionExpr(dim) => valueDataBuilder.set(e, tagValues.get(dim))
          case e @ MetricExpr(field)  => valueDataBuilder.set(e, rowValues(field.tag))
          case TimeExpr               => valueDataBuilder.set(TimeExpr, Some(Time(time)))
          case e                      => throw new IllegalArgumentException(s"Unsupported expression $e passed to DAO")
        }

        valueDataBuilder.buildAndReset()
      }
    }
  }

  override def isSupportedCondition(condition: Condition): Boolean = {
    condition match {
      case BinaryOperationExpr(_, _: TimeExpr.type, ConstantExpr(_)) => true
      case BinaryOperationExpr(_, ConstantExpr(_), _: TimeExpr.type) => true
      case _: DimIdInExpr                                            => true
      case _: DimIdNotInExpr                                         => true
      case Equ(_: DimensionExpr, ConstantExpr(_))                    => true
      case Equ(ConstantExpr(_), _: DimensionExpr)                    => true
      case Neq(_: DimensionExpr, ConstantExpr(_))                    => true
      case Neq(ConstantExpr(_), _: DimensionExpr)                    => true
      case InExpr(_: DimensionExpr, _)                               => true
      case NotInExpr(_: DimensionExpr, _)                            => true
      case _                                                         => false
    }
  }

  private def rowsForDims(
      indexedRows: Seq[(TSDOutputRow[IdType], Int)],
      context: InternalQueryContext
  ): SparseTable[Dimension, IdType, Seq[Int]] = {
    val dimRowMap = context.metricsCollector.dimRowMap.measure(indexedRows.size){
      context.requiredDims.map { dim =>
        val dimIndex = context.dimIndexMap(dim)
        dim -> indexedRows
          .flatMap { case (row, index) => row.key.dimIds(dimIndex).map(index -> _) }
          .groupBy(_._2)
          .mapValues(_.map(_._1))
      }.toMap
    }

    context.metricsCollector.dimRowSparse.measure(dimRowMap.size){
      SparseTable(dimRowMap)
    }
  }

  private def dimFields(
      dimTable: SparseTable[Dimension, IdType, Seq[Int]],
      context: InternalQueryContext
  ): SparseTable[Int, Dimension, String] = {
    val allValues = context.requiredDims.map { dim =>
      val dimIdRows = context.metricsCollector.dimIdRows.measure(dimTable.values.size){
        dimTable.row(dim)
      }
      val dimValues = idsToValues(dim, dimIdRows.keySet, context.metricsCollector)
      context.metricsCollector.handleDimValues.measure(dimValues.size){
        val data = dimValues.flatMap {
          case (dimId, dimValue) =>
            dimIdRows.get(dimId).toSeq.flatMap(_.map(row => (row, dim, dimValue)))
        }
        SparseTable(data)
      }
    }

    context.metricsCollector.handleAllValues.measure(allValues.size){
      allValues.foldLeft(SparseTable.empty[Int, Dimension, String])(_ ++ _)
    }
  }
}
