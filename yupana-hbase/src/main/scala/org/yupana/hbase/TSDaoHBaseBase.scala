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
import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.schema.{ Dimension, Table }
import org.yupana.api.utils.{ DimOrdering, PrefetchedSortedSetIterator, SortedSetIterator }
import org.yupana.core.MapReducible
import org.yupana.core.dao._
import org.yupana.core.model.{ InternalQuery, InternalRow, InternalRowBuilder }
import org.yupana.core.utils.metric.MetricQueryCollector
import org.yupana.core.utils.TimeBoundedCondition
import org.apache.hadoop.hbase.client.{ Result => HResult }

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
  ): Collection[HResult]

  override def query(
      query: InternalQuery,
      internalRowBuilder: InternalRowBuilder,
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
        rs.filter(r => rowFilter(HBaseUtils.parseRowKey(r.getRow, query.table)))
      }

      new TSDHBaseRowIterator(context, filtered.iterator, internalRowBuilder)
        .filter(r => timeFilter(r.get[Time](internalRowBuilder.timeIndex).get.millis))
    }
  }

  def valuesToIds(dimension: Dimension, values: SortedSetIterator[String]): SortedSetIterator[IdType] = {
    val ord = implicitly[DimOrdering[IdType]]
    val dictionary = dictionaryProvider.dictionary(dimension)
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

  private def rowFilter(table: Table, f: (Dimension, IdType) => Boolean): Filtration.RowFilter = { rowKey =>
    rowKey.dimIds.zip(table.dimensionSeq).forall {
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
}
